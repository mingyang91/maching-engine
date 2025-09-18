use std::fmt::Debug;

use chrono::NaiveDateTime;
use sqlx::{Executor, PgPool, Postgres, prelude::FromRow};
use uuid::Uuid;

const THE_OUTSIDE_WORLD: &str = "THE OUTSIDE WORLD";

#[derive(thiserror::Error, Debug)]
pub enum AccountServiceError {
    #[error("db error")]
    DB(#[from] sqlx::Error),
    #[error("insufficient balance for {1}@{0}")]
    InsufficientBalance(String, String),
    #[error("account {1}@{0} not found")]
    AccountNotFound(String, String),
    #[error("transaction#{0} conflict")]
    TransactionConflict(Uuid),
    #[error("transaction#{0} not found")]
    TransactionNotFound(Uuid),
    #[error("illegal state")]
    IllegalState,
}

pub struct AccountService {
    db: PgPool,
}

#[derive(sqlx::Type, Debug, PartialEq, Eq, Clone)]
#[sqlx(type_name = "transaction_type", rename_all = "lowercase")]
enum TransactionType {
    Transfer,
    Deposit,
    Withdrawal,
    Fee,
}

#[derive(sqlx::Type, Debug, PartialEq, Eq, Clone)]
#[sqlx(type_name = "transaction_status", rename_all = "lowercase")]
enum TransactionStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
struct Transaction {
    id: Uuid,
    r#type: TransactionType,
    debitor: String,
    creditor: String,
    asset: String,
    amount: i64,
    status: TransactionStatus,
    extra: Option<serde_json::Value>,
    external_id: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl AccountService {
    pub async fn new(db: PgPool) -> Self {
        Self { db }
    }

    async fn insert_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
        r#type: TransactionType,
        debitor: &str,
        creditor: &str,
        asset: &str,
        amount: u64,
        status: TransactionStatus,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO transactions (id, type, debitor, creditor, asset, amount, status, external_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
            id,
            r#type as TransactionType,
            debitor,
            creditor,
            asset,
            amount as i64,
            status as TransactionStatus,
            external_id,
        )
        .execute(executor)
        .await;

        match res {
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                return Err(AccountServiceError::TransactionConflict(id));
            }
            Err(e) => return Err(e.into()),
            _ => Ok(()),
        }
    }

    async fn lock_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
    ) -> Result<Transaction, AccountServiceError> {
        let res = sqlx::query_as!(
            Transaction,
            r#"
            SELECT
                id,
                type AS "type: TransactionType",
                debitor,
                creditor,
                asset,
                amount,
                status AS "status: TransactionStatus",
                extra,
                external_id,
                created_at,
                updated_at
            FROM transactions 
            WHERE id = $1 
              AND status = $2 
            FOR UPDATE"#,
            id,
            TransactionStatus::Pending as TransactionStatus,
        )
        .fetch_optional(executor)
        .await?;

        match res {
            None => Err(AccountServiceError::TransactionNotFound(id)),
            Some(transaction) => Ok(transaction),
        }
    }

    async fn complete_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE transactions 
            SET status = $1, 
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $2"#,
            TransactionStatus::Completed as TransactionStatus,
            id,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn fail_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE transactions 
            SET status = $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $2"#,
            TransactionStatus::Failed as TransactionStatus,
            id,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn pre_debit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_debit = pending_debit + $1 
            WHERE username = $2 
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.to_string(),
                asset.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn commit_pre_debit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_debit = pending_debit - $1 
            WHERE username = $2 
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP
              AND pending_debit >= $1"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn abort_pre_debit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_debit = pending_debit - $1 
            WHERE username = $2 
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP
              AND pending_debit >= $1"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn pre_credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit + $1 
            WHERE username = $2 
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.to_string(),
                asset.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn commit_pre_credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit - $1,
                balance = balance + $1
            WHERE username = $2 
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP
              AND pending_credit >= $1"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn abort_pre_credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit - $1
            WHERE username = $2
              AND asset = $3 
              AND updated_at = CURRENT_TIMESTAMP
              AND pending_credit >= $1"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn debit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET balance = balance - $1 
            WHERE user = $2 
            AND asset = $3 
            AND balance >= $1 
            AND updated_at = CURRENT_TIMESTAMP"#,
            amount as i64,
            account,
            asset,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::InsufficientBalance(
                account.to_string(),
                asset.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO accounts (username, asset, balance, pending_credit, pending_debit)
            VALUES ($1, $2, $3, 0, 0)
            ON CONFLICT (username, asset) DO UPDATE
            SET balance = EXCLUDED.balance + $3,
                updated_at = CURRENT_TIMESTAMP"#,
            account,
            asset,
            amount as i64,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    pub async fn transfer(
        &self,
        id: Uuid,
        debitor: String,
        creditor: String,
        asset: String,
        amount: u64,
        fee: Option<(String, u64)>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Transfer as TransactionType,
            &debitor,
            &creditor,
            &asset,
            amount,
            TransactionStatus::Completed as TransactionStatus,
            None,
        )
        .await?;

        if let Some((fee_account, fee_amount)) = fee {
            self.insert_transaction(
                &mut *tx,
                id,
                TransactionType::Fee as TransactionType,
                &debitor,
                &fee_account,
                &fee_account,
                fee_amount,
                TransactionStatus::Completed as TransactionStatus,
                None,
            )
            .await?;
        }

        self.debit_account(&mut *tx, &debitor, &debitor, amount)
            .await?;

        self.credit_account(&mut *tx, &creditor, &creditor, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn start_deposit(
        &self,
        id: Uuid,
        account: &str,
        asset: &str,
        amount: u64,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Deposit as TransactionType,
            THE_OUTSIDE_WORLD,
            &account,
            &asset,
            amount,
            TransactionStatus::Pending as TransactionStatus,
            external_id,
        )
        .await?;

        self.pre_credit_account(&mut *tx, &account, &asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.commit_pre_credit_account(
            &mut *tx,
            &transaction.creditor,
            &transaction.asset,
            transaction.amount as u64,
        )
        .await?;

        self.complete_transaction(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn fail_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.abort_pre_credit_account(
            &mut *tx,
            &transaction.creditor,
            &transaction.asset,
            transaction.amount as u64,
        )
        .await?;

        self.fail_transaction(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn start_withdrawal(
        &self,
        id: Uuid,
        account: &str,
        asset: &str,
        amount: u64,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Withdrawal as TransactionType,
            &account,
            THE_OUTSIDE_WORLD,
            &asset,
            amount,
            TransactionStatus::Pending as TransactionStatus,
            None,
        )
        .await?;

        self.pre_debit_account(&mut *tx, &account, &asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.commit_pre_debit_account(
            &mut *tx,
            &transaction.debitor,
            &transaction.asset,
            transaction.amount as u64,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn fail_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.abort_pre_debit_account(
            &mut *tx,
            &transaction.debitor,
            &transaction.asset,
            transaction.amount as u64,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction2() {}
}
