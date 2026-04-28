PRINT '=== Test 6: Verify RLS policies exist in sys.security_policies ==='

SELECT
    p.name          AS policy_name,
    p.is_enabled,
    o.name          AS table_name,
    pred.predicate_definition
FROM sys.security_policies p
JOIN sys.security_predicates pred ON pred.object_id = p.object_id
JOIN sys.objects o ON o.object_id = pred.target_object_id
WHERE p.name IN ('AccountsRLSPolicy', 'TransactionsRLSPolicy', 'UsersRLSPolicy')
ORDER BY p.name;
-- Should return 2 rows, both with is_enabled = 1.

GO