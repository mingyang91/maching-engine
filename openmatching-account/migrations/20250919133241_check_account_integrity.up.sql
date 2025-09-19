-- Add up migration script here

CREATE OR REPLACE FUNCTION check_account_integrity()
RETURNS TABLE (
    username TEXT,
    asset TEXT,
    balance_discrepancy NUMERIC,
    pending_debit_discrepancy NUMERIC,
    pending_credit_discrepancy NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH tx_summary AS (
        SELECT 
            account, 
            asset,
            SUM(amount * CASE 
                WHEN type = 'credit' AND status = 'completed' THEN 1
                WHEN type = 'debit' AND status = 'completed' THEN -1
                ELSE 0
            END) as net_balance,
            SUM(CASE WHEN type = 'debit' AND status = 'pending' THEN amount ELSE 0 END) as pending_d,
            SUM(CASE WHEN type = 'credit' AND status = 'pending' THEN amount ELSE 0 END) as pending_c
        FROM (
            SELECT creditor as account, asset, amount, 'credit' as type, status FROM transactions
            UNION ALL
            SELECT debitor as account, asset, amount, 'debit' as type, status FROM transactions
        ) t
        GROUP BY account, asset
    )
    SELECT 
        a.username,
        a.asset,
        a.balance - COALESCE(t.net_balance, 0),
        a.pending_debit - COALESCE(t.pending_d, 0),
        a.pending_credit - COALESCE(t.pending_c, 0)
    FROM accounts a
    LEFT JOIN tx_summary t ON a.username = t.account AND a.asset = t.asset
    WHERE a.balance <> COALESCE(t.net_balance, 0)
       OR a.pending_debit <> COALESCE(t.pending_d, 0)
       OR a.pending_credit <> COALESCE(t.pending_c, 0);
END;
$$ LANGUAGE plpgsql;