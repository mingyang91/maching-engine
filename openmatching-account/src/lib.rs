use std::{collections::BTreeMap, fmt::Debug, sync::OnceLock};

use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use sqlx::{Executor, PgPool, Postgres, prelude::FromRow};
use uuid::Uuid;

static THE_OUTSIDE_WORLD: OnceLock<Username> = OnceLock::new();
fn the_outside_world() -> &'static Username {
    THE_OUTSIDE_WORLD.get_or_init(|| Username("THE OUTSIDE WORLD".to_string()))
}

#[derive(thiserror::Error, Debug)]
pub enum AccountServiceError {
    #[error("db error: {0}")]
    DB(#[from] sqlx::Error),
    #[error("insufficient balance for {1:?}@{0:?}")]
    InsufficientBalance(Username, Asset),
    #[error("account {1:?}@{0:?} not found")]
    AccountNotFound(Username, Asset),
    #[error("transaction#{0} conflict")]
    TransactionConflict(Uuid),
    #[error("transaction#{0} not found")]
    TransactionNotFound(Uuid),
    #[error("illegal state")]
    IllegalState,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
struct CheckAccountIntegrityResult {
    username: Option<String>,
    asset: Option<String>,
    balance_discrepancy: Option<BigDecimal>,
    pending_debit_discrepancy: Option<BigDecimal>,
    pending_credit_discrepancy: Option<BigDecimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIntegrityResult {
    username: String,
    asset: String,
    balance_discrepancy: BigDecimal,
    pending_debit_discrepancy: BigDecimal,
    pending_credit_discrepancy: BigDecimal,
}

#[derive(thiserror::Error, Debug)]
pub enum AccountIntegrityError {
    #[error("account book balance discrepancy: {0:?}")]
    AccountBookBalanceDiscrepancy(Vec<AccountIntegrityResult>),
    #[error("db error: {0}")]
    DB(#[from] sqlx::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Username(String);

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Asset(String);

pub struct AccountService {
    db: PgPool,
}

#[derive(sqlx::Type, Debug, PartialEq, Eq, Clone)]
#[sqlx(type_name = "transaction_type", rename_all = "lowercase")]
pub enum TransactionType {
    Transfer,
    Deposit,
    Withdrawal,
    Fee,
}

#[derive(sqlx::Type, Debug, PartialEq, Eq, Clone)]
#[sqlx(type_name = "transaction_status", rename_all = "lowercase")]
pub enum TransactionStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TransactionRow {
    id: Uuid,
    r#type: TransactionType,
    debitor: String,
    creditor: String,
    asset: String,
    amount: BigDecimal,
    status: TransactionStatus,
    extra: Option<serde_json::Value>,
    external_id: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct AccountRow {
    username: String,
    asset: String,
    balance: BigDecimal,
    pending_credit: BigDecimal,
    pending_debit: BigDecimal,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl AccountService {
    pub async fn new(db: PgPool) -> Self {
        Self { db }
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
        r#type: TransactionType,
        debitor: &Username,
        creditor: &Username,
        asset: &Asset,
        amount: &BigDecimal,
        status: TransactionStatus,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO transactions (id, type, debitor, creditor, asset, amount, status, external_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
            id,
            r#type as TransactionType,
            debitor.0,
            creditor.0,
            asset.0,
            amount,
            status as TransactionStatus,
            external_id,
        )
        .execute(executor)
        .await;

        match res {
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                Err(AccountServiceError::TransactionConflict(id))
            }
            Err(e) => Err(e.into()),
            _ => Ok(()),
        }
    }

    async fn lock_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
    ) -> Result<TransactionRow, AccountServiceError> {
        let res = sqlx::query_as!(
            TransactionRow,
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
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET 
                balance = balance - $1,
                pending_debit = pending_debit + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3 
              AND balance >= $1"#,
            amount,
            account.0,
            asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.clone(),
                asset.clone(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn commit_pre_debit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_debit = pending_debit - $1 
            WHERE username = $2 
              AND asset = $3 
              AND pending_debit >= $1"#,
            amount,
            account.0,
            asset.0,
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
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET 
                pending_debit = pending_debit - $1,
                balance = balance + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3 
              AND pending_debit >= $1"#,
            amount,
            account.0,
            asset.0,
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
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3"#,
            amount,
            account.0,
            asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.clone(),
                asset.clone(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn commit_pre_credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit - $1,
                balance = balance + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3 
              AND pending_credit >= $1"#,
            amount,
            account.0,
            asset.0,
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
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET pending_credit = pending_credit - $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2
              AND asset = $3 
              AND pending_credit >= $1"#,
            amount,
            account.0,
            asset.0,
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
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET balance = balance - $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
            AND asset = $3 
            AND balance >= $1"#,
            amount,
            account.0,
            asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::InsufficientBalance(
                account.clone(),
                asset.clone(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO accounts (username, asset, balance, pending_credit, pending_debit)
            VALUES ($1, $2, $3, 0, 0)
            ON CONFLICT (username, asset) DO UPDATE
            SET balance = EXCLUDED.balance + $3,
                updated_at = CURRENT_TIMESTAMP"#,
            account.0,
            asset.0,
            amount,
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
        debitor: &Username,
        creditor: &Username,
        asset: &Asset,
        amount: &BigDecimal,
        fee: Option<(Username, Asset, &BigDecimal)>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Transfer as TransactionType,
            debitor,
            creditor,
            asset,
            amount,
            TransactionStatus::Completed as TransactionStatus,
            None,
        )
        .await?;

        if let Some((fee_account, fee_asset, fee_amount)) = fee {
            self.insert_transaction(
                &mut *tx,
                id,
                TransactionType::Fee as TransactionType,
                debitor,
                &fee_account,
                &fee_asset,
                fee_amount,
                TransactionStatus::Completed as TransactionStatus,
                None,
            )
            .await?;
        }

        self.debit_account(&mut *tx, debitor, asset, amount).await?;

        self.credit_account(&mut *tx, creditor, asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn start_deposit(
        &self,
        id: Uuid,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Deposit as TransactionType,
            the_outside_world(),
            account,
            asset,
            amount,
            TransactionStatus::Pending as TransactionStatus,
            external_id,
        )
        .await?;

        self.pre_credit_account(&mut *tx, account, asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.commit_pre_credit_account(
            &mut *tx,
            &Username(transaction.creditor.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
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
            &Username(transaction.creditor.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        self.fail_transaction(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn start_withdrawal(
        &self,
        id: Uuid,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        self.insert_transaction(
            &mut *tx,
            id,
            TransactionType::Withdrawal as TransactionType,
            account,
            the_outside_world(),
            asset,
            amount,
            TransactionStatus::Pending as TransactionStatus,
            None,
        )
        .await?;

        self.pre_debit_account(&mut *tx, account, asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.commit_pre_debit_account(
            &mut *tx,
            &Username(transaction.debitor.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        self.complete_transaction(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn fail_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.abort_pre_debit_account(
            &mut *tx,
            &Username(transaction.debitor.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_asset_account(
        &self,
        account: &Username,
        asset: &Asset,
    ) -> Result<Option<AccountRow>, AccountServiceError> {
        let res = sqlx::query_as!(
            AccountRow,
            r#"SELECT * FROM accounts WHERE username = $1 AND asset = $2"#,
            account.0,
            asset.0,
        )
        .fetch_optional(&self.db)
        .await?;

        Ok(res)
    }

    pub async fn get_account(
        &self,
        account: &Username,
    ) -> Result<BTreeMap<Asset, AccountRow>, AccountServiceError> {
        let res: Vec<AccountRow> = sqlx::query_as!(
            AccountRow,
            r#"SELECT * FROM accounts WHERE username = $1"#,
            account.0,
        )
        .fetch_all(&self.db)
        .await?;

        Ok(res
            .into_iter()
            .map(|row| (Asset(row.asset.clone()), row))
            .collect())
    }

    pub async fn check_account_integrity(&self) -> Result<(), AccountIntegrityError> {
        let res: Vec<CheckAccountIntegrityResult> = sqlx::query_as!(
            CheckAccountIntegrityResult,
            r#"SELECT * FROM check_account_integrity()"#,
        )
        .fetch_all(&self.db)
        .await?;

        let res: Vec<AccountIntegrityResult> = res
            .into_iter()
            .map(|row| AccountIntegrityResult {
                username: row.username.expect("username is not null"),
                asset: row.asset.expect("asset is not null"),
                balance_discrepancy: row
                    .balance_discrepancy
                    .expect("balance_discrepancy is not null"),
                pending_debit_discrepancy: row
                    .pending_debit_discrepancy
                    .expect("pending_debit_discrepancy is not null"),
                pending_credit_discrepancy: row
                    .pending_credit_discrepancy
                    .expect("pending_credit_discrepancy is not null"),
            })
            .collect();

        if res.is_empty() {
            Ok(())
        } else {
            Err(AccountIntegrityError::AccountBookBalanceDiscrepancy(res))
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_transaction2() {}
}
