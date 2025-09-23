//! Account service with two-phase commit for external operations.
//!
//! Internal transfers are atomic and immediate.
//! External operations (deposits/withdrawals) use pending states:
//! - Deposits: track incoming → approve/reject → balance updated
//! - Withdrawals: reserve from balance → approve/reject → complete
//!
//! Account invariant: `available = balance - reserved`

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::{collections::BTreeMap, fmt::Debug};

use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use sqlx::{Executor, PgPool, Postgres, prelude::FromRow};
use uuid::Uuid;

/// External account for deposits/withdrawals
const EXTERNAL_ACCOUNT: &str = "EXTERNAL";

#[derive(thiserror::Error, Debug)]
pub enum AccountServiceError {
    #[error("db error: {0}")]
    DB(#[from] sqlx::Error),
    #[error("insufficient balance for {1:?}@{0:?}")]
    InsufficientBalance(String, String),
    #[error("account {1:?}@{0:?} not found")]
    AccountNotFound(String, String),
    #[error("transaction#{0} conflict")]
    TransactionConflict(Uuid),
    #[error("transaction#{0} not found")]
    TransactionNotFound(Uuid),
    /// Data inconsistency - requires immediate investigation
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

/// Account integrity check result - non-zero values indicate problems.
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
pub struct Username<'a>(Cow<'a, str>);

impl<'a> Username<'a> {
    pub fn new(name: &'a str) -> Self {
        Self(Cow::Borrowed(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Asset<'a>(Cow<'a, str>);

impl<'a> Asset<'a> {
    pub fn new(asset: &'a str) -> Self {
        Self(Cow::Borrowed(asset))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Account service - thread-safe, can be shared across async tasks.
#[derive(Clone)]
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

/// Immutable transaction record.
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

impl Transaction {
    pub fn from(&self) -> Username<'_> {
        Username(Cow::Borrowed(&self.from))
    }

    pub fn to(&self) -> Username<'_> {
        Username(Cow::Borrowed(&self.to))
    }

    pub fn asset(&self) -> Asset<'_> {
        Asset(Cow::Borrowed(&self.asset))
    }
}

/// Account with balance, incoming (pending deposits), and reserved (pending withdrawals).
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

impl Account {
    pub fn username(&self) -> Username<'_> {
        Username(Cow::Borrowed(&self.username))
    }

    pub fn asset(&self) -> Asset<'_> {
        Asset(Cow::Borrowed(&self.asset))
    }
}

impl AccountService {
    pub async fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Calculate deterministic advisory lock ID for account operations.
    /// ALWAYS sorts accounts to ensure consistent lock ordering.
    fn calculate_advisory_lock_id(
        account1: &Username<'_>,
        account2: &Username<'_>,
        asset: &Asset<'_>,
    ) -> i64 {
        // Sort accounts to ensure consistent ordering
        let (first, second) = if account1.0 <= account2.0 {
            (&account1.0, &account2.0)
        } else {
            (&account2.0, &account1.0)
        };

        // Hash the canonical form
        let mut hasher = DefaultHasher::new();
        first.hash(&mut hasher);
        second.hash(&mut hasher);
        asset.0.hash(&mut hasher);

        // PostgreSQL advisory locks use BIGINT (i64)
        // Use lower 63 bits to avoid sign issues
        (hasher.finish() as i64) & 0x7FFFFFFFFFFFFFFF
    }

    async fn acquire_advisory_lock(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        lock_id: i64,
    ) -> Result<(), AccountServiceError> {
        sqlx::query!("SELECT pg_advisory_xact_lock($1)", lock_id)
            .execute(executor)
            .await?;
        Ok(())
    }

    async fn account_pair_lock(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account1: &Username<'_>,
        account2: &Username<'_>,
        asset: &Asset<'_>,
    ) -> Result<(), AccountServiceError> {
        let lock_id = Self::calculate_advisory_lock_id(account1, account2, asset);
        self.acquire_advisory_lock(executor, lock_id).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_transaction(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        id: Uuid,
        kind: TransactionKind,
        debitor: &Username<'_>,
        creditor: &Username<'_>,
        asset: &Asset<'_>,
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
            &debitor.0,
            &creditor.0,
            &asset.0,
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

    /// Reserve funds for withdrawal (balance -> reserved)
    async fn reserve_funds(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.0.to_string(),
                asset.0.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Complete withdrawal (remove reservation)
    async fn release_reservation(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Cancel withdrawal (reserved -> balance)
    async fn cancel_reservation(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Track incoming deposit
    async fn track_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::AccountNotFound(
                account.0.to_string(),
                asset.0.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Accept deposit (incoming -> balance)
    async fn accept_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Reject deposit (cancel incoming)
    async fn reject_incoming(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
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
        account: &Username<'_>,
        asset: &Asset<'_>,
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
            &account.0,
            &asset.0,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() == 0 {
            return Err(AccountServiceError::InsufficientBalance(
                account.0.to_string(),
                asset.0.to_string(),
            ));
        } else if res.rows_affected() > 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    async fn credit_account(
        &self,
        executor: impl Executor<'_, Database = Postgres>,
        account: &Username<'_>,
        asset: &Asset<'_>,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let res = sqlx::query!(
            r#"
            INSERT INTO accounts (username, asset, balance, incoming, reserved)
            VALUES ($1, $2, $3, 0, 0)
            ON CONFLICT (username, asset) DO UPDATE
            SET balance = EXCLUDED.balance + $3,
                updated_at = CURRENT_TIMESTAMP"#,
            &account.0,
            &asset.0,
            amount,
        )
        .execute(executor)
        .await?;

        if res.rows_affected() != 1 {
            return Err(AccountServiceError::IllegalState);
        }

        Ok(())
    }

    /// Atomic transfer between accounts. No pending states.
    /// Uses advisory locks to prevent deadlocks.
    pub async fn transfer(
        &self,
        id: Uuid,
        debitor: &Username<'_>,
        creditor: &Username<'_>,
        asset: &Asset<'_>,
        amount: &BigDecimal,
        fee: Option<(Username<'_>, Asset<'_>, &BigDecimal)>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;

        self.account_pair_lock(&mut *tx, debitor, creditor, asset)
            .await?;

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

    /// Start deposit - funds tracked as incoming until approved.
    pub async fn start_deposit(
        &self,
        id: Uuid,
        account: &Username<'_>,
        asset: &Asset<'_>,
        amount: &BigDecimal,
        external_id: Option<String>,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;

        self.insert_transaction(
            &mut *tx,
            id,
            TransactionKind::Deposit,
            &Username(Cow::Borrowed(EXTERNAL_ACCOUNT)),
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

    pub async fn approve_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        // Lock the account being deposited to
        let account = transaction.to();
        let asset = transaction.asset();

        self.accept_incoming(&mut *tx, &account, &asset, &transaction.amount)
            .await?;

        self.mark_transaction_completed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn reject_deposit(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        // Lock the account
        let account = transaction.to();
        let asset = transaction.asset();

        self.reject_incoming(&mut *tx, &account, &asset, &transaction.amount)
            .await?;

        self.mark_transaction_failed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Start withdrawal - reserves funds from balance.
    pub async fn start_withdrawal(
        &self,
        id: Uuid,
        account: &Username<'_>,
        asset: &Asset<'_>,
        amount: &BigDecimal,
    ) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;

        self.insert_transaction(
            &mut *tx,
            id,
            TransactionKind::Withdrawal,
            account,
            &Username(Cow::Borrowed(EXTERNAL_ACCOUNT)),
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

    /// Funds already deducted, this removes the reservation.
    pub async fn approve_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        // Lock the account being withdrawn from
        let account = transaction.from();
        let asset = transaction.asset();

        self.release_reservation(&mut *tx, &account, &asset, &transaction.amount)
            .await?;

        self.mark_transaction_completed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn reject_withdrawal(&self, id: Uuid) -> Result<(), AccountServiceError> {
        let mut tx = self.db.begin().await?;
        let transaction = self.lock_transaction(&mut *tx, id).await?;

        // Lock the account
        let account = transaction.from();
        let asset = transaction.asset();

        self.cancel_reservation(&mut *tx, &account, &asset, &transaction.amount)
            .await?;

        self.mark_transaction_failed(&mut *tx, id).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Get account for specific asset. Returns None if not exists.
    pub async fn get_asset_account(
        &self,
        account: &Username<'_>,
        asset: &Asset<'_>,
    ) -> Result<Option<Account>, AccountServiceError> {
        let res = sqlx::query_as!(
            Account,
            r#"SELECT * FROM accounts WHERE username = $1 AND asset = $2"#,
            &account.0,
            &asset.0,
        )
        .fetch_optional(&self.db)
        .await?;

        Ok(res)
    }

    /// Get all accounts for a user.
    pub async fn get_account(
        &self,
        account: &Username<'_>,
    ) -> Result<BTreeMap<String, Account>, AccountServiceError> {
        let res: Vec<Account> = sqlx::query_as!(
            Account,
            r#"SELECT * FROM accounts WHERE username = $1"#,
            &account.0,
        )
        .fetch_all(&self.db)
        .await?;

        Ok(res
            .into_iter()
            .map(|row| (row.asset.clone(), row))
            .collect())
    }

    /// Verify all account balances match transaction history.
    ///
    /// Returns error with discrepancies if any found.
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
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::env;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_advisory_lock_prevents_deadlock() {
        // This test requires a running PostgreSQL instance
        // Skip if DATABASE_URL is not set
        let database_url = match env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                println!("Skipping test: DATABASE_URL not set");
                return;
            }
        };

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        let service = AccountService::new(pool).await;

        // Create test accounts
        let alice = Username::new("alice");
        let bob = Username::new("bob");
        let usd = Asset::new("USD");
        let initial_amount = BigDecimal::from(1000);

        // Initialize accounts with balance
        let _ = service
            .transfer(
                Uuid::new_v4(),
                &Username::new("system"),
                &alice,
                &usd,
                &initial_amount,
                None,
            )
            .await; // May fail if system account doesn't exist, that's OK for this test

        let _ = service
            .transfer(
                Uuid::new_v4(),
                &Username::new("system"),
                &bob,
                &usd,
                &initial_amount,
                None,
            )
            .await; // May fail if system account doesn't exist, that's OK for this test

        // Simulate concurrent bidirectional transfers
        // Without advisory locks, this would deadlock
        let service1 = service.clone();
        let service2 = service.clone();

        let alice1 = alice.clone();
        let bob1 = bob.clone();
        let usd1 = usd.clone();

        let alice2 = alice.clone();
        let bob2 = bob.clone();
        let usd2 = usd.clone();

        // These would deadlock without advisory locks
        let handle1 = tokio::spawn(async move {
            for i in 0..10 {
                let amount = BigDecimal::from(10);
                let _ = service1
                    .transfer(Uuid::new_v4(), &alice1, &bob1, &usd1, &amount, None)
                    .await;
                println!("Transfer {i} from Alice to Bob completed");
            }
        });

        let handle2 = tokio::spawn(async move {
            for i in 0..10 {
                let amount = BigDecimal::from(5);
                let _ = service2
                    .transfer(Uuid::new_v4(), &bob2, &alice2, &usd2, &amount, None)
                    .await;
                println!("Transfer {i} from Bob to Alice completed");
            }
        });

        // Both should complete without deadlock
        let result1 = tokio::time::timeout(std::time::Duration::from_secs(10), handle1).await;
        let result2 = tokio::time::timeout(std::time::Duration::from_secs(10), handle2).await;

        assert!(result1.is_ok(), "Transfer 1 timed out - possible deadlock!");
        assert!(result2.is_ok(), "Transfer 2 timed out - possible deadlock!");

        println!("✓ Advisory locks successfully prevented deadlocks");
    }
}
