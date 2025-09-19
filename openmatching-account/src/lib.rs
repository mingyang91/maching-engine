//! Account service for managing user balances and transactions
//!
//! Key concepts:
//! - `reserve_funds`: Lock funds for pending withdrawals
//! - `track_incoming`: Track pending deposits
//! - Internal transfers are immediate, external operations use pending states

use std::{collections::BTreeMap, fmt::Debug};

use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use sqlx::{Executor, PgPool, Postgres, prelude::FromRow};
use uuid::Uuid;

/// External account identifier for deposits/withdrawals
const EXTERNAL_ACCOUNT: &str = "EXTERNAL";

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
    reserved_discrepancy: Option<BigDecimal>,
    incoming_discrepancy: Option<BigDecimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIntegrityResult {
    username: String,
    asset: String,
    balance_discrepancy: BigDecimal,
    reserved_discrepancy: BigDecimal,
    incoming_discrepancy: BigDecimal,
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
#[sqlx(type_name = "transaction_kind", rename_all = "lowercase")]
pub enum TransactionKind {
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
pub struct Transaction {
    id: Uuid,
    kind: TransactionKind,
    #[sqlx(rename = "sender")]
    from: String,
    #[sqlx(rename = "receiver")]
    to: String,
    asset: String,
    amount: BigDecimal,
    status: TransactionStatus,
    extra: Option<serde_json::Value>,
    external_id: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct Account {
    username: String,
    asset: String,
    balance: BigDecimal,
    incoming: BigDecimal, // funds tracked for pending deposits
    reserved: BigDecimal, // funds reserved for pending withdrawals
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
        kind: TransactionKind,
        debitor: &Username,
        creditor: &Username,
        asset: &Asset,
        amount: &BigDecimal,
        status: TransactionStatus,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO transactions (id, kind, sender, receiver, asset, amount, status, external_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
            id,
            kind as _,
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
    ) -> Result<Transaction, AccountServiceError> {
        let res = sqlx::query_as!(
            Transaction,
            r#"
            SELECT
                id,
                kind AS "kind!: TransactionKind",
                sender AS "from",
                receiver AS "to",
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
            TransactionStatus::Pending as _,
        )
        .fetch_optional(executor)
        .await?;

        match res {
            None => Err(AccountServiceError::TransactionNotFound(id)),
            Some(transaction) => Ok(transaction),
        }
    }

    async fn mark_transaction_completed(
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
            TransactionStatus::Completed as _,
            id,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn mark_transaction_failed(
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
            TransactionStatus::Failed as _,
            id,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Reserve funds for a pending withdrawal (deducts from balance, adds to pending_debit)
    async fn reserve_funds(
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
                reserved = reserved + $1,
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

    /// Complete a withdrawal by releasing the reservation (removes from pending_debit)
    async fn release_reservation(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET reserved = reserved - $1 
            WHERE username = $2 
              AND asset = $3 
              AND reserved >= $1"#,
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

    /// Cancel a withdrawal by returning reserved funds (removes from pending_debit, returns to balance)
    async fn cancel_reservation(
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
                reserved = reserved - $1,
                balance = balance + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3 
              AND reserved >= $1"#,
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

    /// Track incoming funds from a deposit (adds to pending_credit)
    async fn track_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET incoming = incoming + $1,
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

    /// Accept incoming funds and add to balance (moves from pending_credit to balance)
    async fn accept_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET incoming = incoming - $1,
                balance = balance + $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2 
              AND asset = $3 
              AND incoming >= $1"#,
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

    /// Reject incoming funds (removes from pending_credit)
    async fn reject_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username,
        asset: &Asset,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            UPDATE accounts 
            SET incoming = incoming - $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE username = $2
              AND asset = $3 
              AND incoming >= $1"#,
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
            INSERT INTO accounts (username, asset, balance, incoming, reserved)
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

    /// Perform an internal transfer between accounts (immediate, no pending state)
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
            TransactionKind::Transfer,
            debitor,
            creditor,
            asset,
            amount,
            TransactionStatus::Completed,
            None,
        )
        .await?;

        if let Some((fee_account, fee_asset, fee_amount)) = fee {
            self.insert_transaction(
                &mut *tx,
                id,
                TransactionKind::Fee,
                debitor,
                &fee_account,
                &fee_asset,
                fee_amount,
                TransactionStatus::Completed,
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

    /// Start a deposit from external source (funds tracked as pending until confirmed)
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
            TransactionKind::Deposit,
            &Username(EXTERNAL_ACCOUNT.to_string()),
            account,
            asset,
            amount,
            TransactionStatus::Pending,
            external_id,
        )
        .await?;

        self.track_incoming(&mut *tx, account, asset, amount)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Approve a pending deposit (moves funds from pending to available balance)
    pub async fn approve_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.accept_incoming(
            &mut *tx,
            &Username(transaction.to.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        self.mark_transaction_completed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Reject a pending deposit (cancels the pending credit)
    pub async fn reject_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.reject_incoming(
            &mut *tx,
            &Username(transaction.to.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        self.mark_transaction_failed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Start a withdrawal to external destination (reserves funds from balance)
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
            TransactionKind::Withdrawal,
            account,
            &Username(EXTERNAL_ACCOUNT.to_string()),
            asset,
            amount,
            TransactionStatus::Pending,
            None,
        )
        .await?;

        self.reserve_funds(&mut *tx, account, asset, amount).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Approve a pending withdrawal (completes the fund reservation)
    pub async fn approve_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.release_reservation(
            &mut *tx,
            &Username(transaction.from.clone()),
            &Asset(transaction.asset.clone()),
            &transaction.amount,
        )
        .await?;

        self.mark_transaction_completed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Reject a pending withdrawal (returns reserved funds to balance)
    pub async fn reject_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        self.cancel_reservation(
            &mut *tx,
            &Username(transaction.from.clone()),
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
    ) -> Result<Option<Account>, AccountServiceError> {
        let res = sqlx::query_as!(
            Account,
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
    ) -> Result<BTreeMap<Asset, Account>, AccountServiceError> {
        let res: Vec<Account> = sqlx::query_as!(
            Account,
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
                reserved_discrepancy: row
                    .reserved_discrepancy
                    .expect("reserved_discrepancy is not null"),
                incoming_discrepancy: row
                    .incoming_discrepancy
                    .expect("incoming_discrepancy is not null"),
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
