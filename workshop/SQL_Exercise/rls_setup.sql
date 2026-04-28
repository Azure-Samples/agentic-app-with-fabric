-- =============================================================================
-- Row Level Security (RLS) Setup for agentic_app_db
-- =============================================================================
-- Run this script once on your Fabric SQL database to enforce per-user data
-- isolation. The app sets SESSION_CONTEXT(N'user_id') before every raw query,
-- and these policies ensure no row is visible unless it belongs to that user.
--
-- Tables protected:
--   dbo.users        – user can only see their own profile row (PK = id)
--   dbo.accounts     – has a direct user_id column
--   dbo.transactions – linked to accounts; visible only if the user owns
--                      at least one of from_account_id / to_account_id
--
-- To DISABLE policies temporarily (e.g. for admin seeding):
--   ALTER SECURITY POLICY UsersRLSPolicy        WITH (STATE = OFF);
--   ALTER SECURITY POLICY AccountsRLSPolicy     WITH (STATE = OFF);
--   ALTER SECURITY POLICY TransactionsRLSPolicy WITH (STATE = OFF);
-- Re-enable after:
--   ALTER SECURITY POLICY UsersRLSPolicy        WITH (STATE = ON);
--   ALTER SECURITY POLICY AccountsRLSPolicy     WITH (STATE = ON);
--   ALTER SECURITY POLICY TransactionsRLSPolicy WITH (STATE = ON);
-- =============================================================================

-- Step 1: Create Security schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Security')
    EXEC('CREATE SCHEMA Security');
GO

-- =============================================================================
-- Step 2: Filter predicate for dbo.users
-- dbo.users has no user_id column — its PK column 'id' IS the user identity.
-- A user can only see their own profile row.
-- =============================================================================
CREATE OR ALTER FUNCTION Security.fn_users_rls_predicate(@row_id NVARCHAR(255))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS predicate_result
    WHERE @row_id = CAST(SESSION_CONTEXT(N'user_id') AS NVARCHAR(255));
GO

-- =============================================================================
-- Step 3: Filter predicate for dbo.accounts
-- The function receives the row's user_id column and returns 1 (visible) only
-- when it matches the SESSION_CONTEXT value set by the application.
-- =============================================================================
CREATE OR ALTER FUNCTION Security.fn_accounts_rls_predicate(@row_user_id NVARCHAR(255))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS predicate_result
    WHERE @row_user_id = CAST(SESSION_CONTEXT(N'user_id') AS NVARCHAR(255));
GO

-- =============================================================================
-- Step 4: Filter predicate for dbo.transactions
-- Transactions have no direct user_id. A transaction is visible to a user if
-- they own the source account OR the destination account.
-- NULL account IDs (e.g. external deposits) are handled safely: NULL = X is
-- NULL (not TRUE), so the branch simply doesn't match — no false positives.
-- =============================================================================
CREATE OR ALTER FUNCTION Security.fn_transactions_rls_predicate(
    @from_account_id NVARCHAR(255),
    @to_account_id   NVARCHAR(255)
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS predicate_result
    WHERE EXISTS (
        SELECT 1
        FROM dbo.accounts a
        WHERE a.user_id = CAST(SESSION_CONTEXT(N'user_id') AS NVARCHAR(255))
          AND (a.id = @from_account_id OR a.id = @to_account_id)
    );
GO

-- =============================================================================
-- Step 5: Security policy on dbo.users
-- NOTE: passes the 'id' column (the PK), not 'user_id' — that column
--       does not exist on dbo.users.
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'UsersRLSPolicy')
    DROP SECURITY POLICY UsersRLSPolicy;
GO

CREATE SECURITY POLICY UsersRLSPolicy
    ADD FILTER PREDICATE Security.fn_users_rls_predicate(id)
    ON dbo.users
WITH (STATE = ON);
GO

-- =============================================================================
-- Step 6: Security policy on dbo.accounts
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'AccountsRLSPolicy')
    DROP SECURITY POLICY AccountsRLSPolicy;
GO

CREATE SECURITY POLICY AccountsRLSPolicy
    ADD FILTER PREDICATE Security.fn_accounts_rls_predicate(user_id)
    ON dbo.accounts
WITH (STATE = ON);
GO

-- =============================================================================
-- Step 7: Security policy on dbo.transactions
-- =============================================================================
IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'TransactionsRLSPolicy')
    DROP SECURITY POLICY TransactionsRLSPolicy;
GO

CREATE SECURITY POLICY TransactionsRLSPolicy
    ADD FILTER PREDICATE Security.fn_transactions_rls_predicate(from_account_id, to_account_id)
    ON dbo.transactions
WITH (STATE = ON);
GO

PRINT 'RLS policies created successfully on dbo.users, dbo.accounts, and dbo.transactions.';
GO
