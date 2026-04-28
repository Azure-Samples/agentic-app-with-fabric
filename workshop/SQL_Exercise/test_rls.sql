-- =============================================================================
-- RLS Verification Script
-- =============================================================================
-- Run this directly against agentic_app_db (SSMS, Azure Data Studio, sqlcmd).
-- It verifies that the SESSION_CONTEXT-based RLS policies are enforcing
-- per-user data isolation at the database layer.
--
-- Expected results are described in the PRINT statements before each block.
-- =============================================================================

PRINT '=== Test 1: No SESSION_CONTEXT set — expect 0 rows from both tables ==='
-- Without a user_id in SESSION_CONTEXT the predicates evaluate to NULL (not TRUE),
-- so every row is invisible.

SELECT COUNT(*) AS accounts_visible_without_context    FROM dbo.accounts;
SELECT COUNT(*) AS transactions_visible_without_context FROM dbo.transactions;
-- Both should be 0.
GO

-- ─────────────────────────────────────────────────────────────────────────────

PRINT '=== Test 2: Set context to a known user — expect ONLY that user''s rows ==='

EXEC sys.sp_set_session_context @key = N'user_id', @value = N'user_5';

SELECT
    user_id,
    COUNT(*) AS account_count
FROM dbo.accounts
GROUP BY user_id;
-- Should show exactly one group: user_id = 'user_5'.

GO

-- ─────────────────────────────────────────────────────────────────────────────

PRINT '=== Test 3: Cross-user attempt — WHERE clause cannot bypass RLS ==='

EXEC sys.sp_set_session_context @key = N'user_id', @value = N'user_5';

-- Even though the WHERE clause asks for a different user, RLS filters first.
-- Replace 'user_1' with any other user_id that exists in your database.
SELECT COUNT(*) AS cross_user_rows
FROM dbo.accounts
WHERE user_id = 'user_1';
-- Should be 0 — the RLS predicate already excluded those rows.

GO

-- ─────────────────────────────────────────────────────────────────────────────

PRINT '=== Test 4: Transactions visible only for the context user''s accounts ==='

EXEC sys.sp_set_session_context @key = N'user_id', @value = N'user_5';

SELECT
    t.id,
    t.amount,
    t.type,
    t.description
FROM dbo.transactions t
ORDER BY t.created_at DESC;
-- Should only include transactions where from_account_id or to_account_id
-- belongs to user_5.  Confirm by spot-checking a few account IDs.

GO

-- ─────────────────────────────────────────────────────────────────────────────

PRINT '=== Test 5: Switching context gives a different user''s rows ==='

-- Set context to a second user (replace with a real user_id from your database).
EXEC sys.sp_set_session_context @key = N'user_id', @value = N'user_2';

SELECT
    user_id,
    COUNT(*) AS account_count
FROM dbo.accounts
GROUP BY user_id;
-- Should show exactly one group: user_id = 'user_2'.
-- If user_2 has no accounts the result will be an empty set — that is correct.

GO

-- ─────────────────────────────────────────────────────────────────────────────

PRINT '=== Test 6: Verify RLS policies exist in sys.security_policies ==='

SELECT
    p.name          AS policy_name,
    p.is_enabled,
    o.name          AS table_name,
    pred.predicate_definition
FROM sys.security_policies p
JOIN sys.security_predicates pred ON pred.object_id = p.object_id
JOIN sys.objects o ON o.object_id = pred.target_object_id
WHERE p.name IN ('AccountsRLSPolicy', 'TransactionsRLSPolicy')
ORDER BY p.name;
-- Should return 2 rows, both with is_enabled = 1.

GO
