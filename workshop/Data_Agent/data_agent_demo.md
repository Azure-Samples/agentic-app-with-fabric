# Fabric Data Agent: Before & After Configuration Demo

## Overview

This document demonstrates how proper configuration of a Fabric Data Agent dramatically improves query accuracy. We walk through **5 realistic banking queries** showing how the agent fails with minimal setup, and succeeds after applying structured instructions, data source context, and targeted few-shot examples.

> **Key insight**: The Data Agent is only as good as the context you give it. Without instructions, it guesses at joins, misinterprets columns, and returns wrong results — even though the data is right there.

---

## What Changed

| Configuration Lever | Before (Default) | After (Optimized) |
|---|---|---|
| **Agent Instructions** | Flat text describing tables and joins (~857 chars) | Structured with `## Role`, `## Response Format`, `## Routing` headers (P1 pattern: +7.8% accuracy) |
| **DS Instructions** | `null` — completely empty | Join paths, business rules, terminology, user context (~2000 chars, under 3000 sweet spot) |
| **Few-Shot Examples** | 2 generic examples with `user_123` placeholder | 8 targeted examples using real user IDs covering key query patterns |
| **Table Descriptions** | None | Added for `users`, `accounts`, `transactions` |
| **Column Selection** | `description` column missing from config | All columns now included |

### Original Agent Instructions (Before)
```
- user table has basic user information.
- account table has info on all accounts each user has
- transactions table has all transactions going from source account to
  destination account. Source account id is populated in "from_account_id"
  and destination account id is "to_account_id".
- account table can be joined with users table using user_id column as the
  common key (equivalant to id column in users table).
- transactions table cannot be directly joined to users table. you must first
  join transactions table with accounts table using from_account_id or
  to_account_id which are equivalant to the id column in accounts tablel;
  then you can join the the results with user_id.
```

### Original Few-Shots (Before)
Just 2 examples, both using a non-existent `user_123`:
1. *"How many accounts does this user have?"* → simple COUNT
2. *"Show my transactions in the groceries category"* → uses `OR` join + `LOWER()` (can cause duplicates & perf issues)

### Original DS-Level Instructions (Before)
```
null
```
*Nothing.* The agent had zero context about business rules, terminology, or column semantics.

---

## Demo Scenarios

### Demo 1: "What is my total spending by category?"

**Why it's hard**: Requires joining `transactions → accounts → users`, filtering by `type = 'payment'`, and aggregating by category. Without guidance, the agent doesn't know that "spending" = payments only (not deposits or transfers).

#### ❌ Before (No DS Instructions, No Few-Shots)
The agent might:
- Include ALL transaction types (payments + deposits + transfers) in "spending"
- Join on both `from_account_id` and `to_account_id`, double-counting transfers
- Not know to filter `type = 'payment'`

```sql
-- Likely generated (WRONG): includes deposits and transfers as "spending"
SELECT t.category, SUM(t.amount) AS total
FROM dbo.transactions t
JOIN dbo.accounts a ON t.from_account_id = a.id OR t.to_account_id = a.id
WHERE a.user_id = 'user_1'
GROUP BY t.category
```
**Result**: Inflated totals, Transfer category shows up as "spending", Income counted as expense.

#### ✅ After (With DS Instructions + Few-Shot)
DS instructions define: *"Total spending = SUM(amount) WHERE type = 'payment'"*
Few-shot #3 provides the exact pattern.

```sql
-- Generated (CORRECT): filters to payments only, joins on from_account_id
SELECT t.category, COUNT(*) AS num_transactions, SUM(t.amount) AS total_amount
FROM dbo.transactions t
INNER JOIN dbo.accounts a ON t.from_account_id = a.id
WHERE a.user_id = 'user_1' AND t.type = 'payment'
GROUP BY t.category
ORDER BY total_amount DESC
```
**Result**: Accurate breakdown — Housing $1,200, Shopping $200, Food $175.50, Groceries $75.

---

### Demo 2: "What are my account balances?"

**Why it's hard**: Simple query, but without context the agent doesn't know account types matter or how to present the data meaningfully.

#### ❌ Before (Minimal Config)
The agent might:
- Return just a single SUM (losing per-account detail)
- Not include account type or name
- Miss that credit card balances are negative (amount owed)

```sql
-- Likely generated (UNHELPFUL): just one number
SELECT SUM(balance) FROM dbo.accounts WHERE user_id = 'user_2'
```
**Result**: Returns `$11,500.75` — user has no idea which accounts have what.

#### ✅ After (With DS Instructions + Few-Shot)  
DS instructions define: *"balance: positive for checking/savings, negative for credit cards (amount owed)"*  
Few-shot #4 shows the correct pattern.

```sql
-- Generated (CORRECT): shows each account with type and name
SELECT a.name, a.account_type, a.balance, a.account_number
FROM dbo.accounts a
WHERE a.user_id = 'user_2'
ORDER BY a.account_type
```
**Result**: 
| name | account_type | balance | account_number |
|------|-------------|---------|----------------|
| Everyday Checking | checking | $3,050.75 | 223344556 |
| Vacation Savings | savings | $8,450.00 | 665544332 |

---

### Demo 3: "Show my recent transactions"

**Why it's hard**: Requires the correct join path through accounts, and ordering by `created_at`. The original config uses `OR` joins which can produce **duplicate rows** for transfers between a user's own accounts.

#### ❌ Before (OR Join Pattern from Original Few-Shot)
```sql
-- Original few-shot taught this pattern (PROBLEMATIC)
SELECT t.id, t.amount, t.type, t.category, t.status, t.created_at
FROM dbo.transactions t
INNER JOIN dbo.accounts a ON t.from_account_id = a.id OR t.to_account_id = a.id
WHERE a.user_id = 'user_3'
```
**Problem**: If user_3 transfers money between their own checking and savings account, that transaction matches TWICE (once for each account). The same transaction appears as two rows.

**Also**: Selecting `created_at` (a `datetimeoffset` type) can cause client-side errors in some drivers.

#### ✅ After (Clean Join, No Duplicates)
```sql
-- Generated (CORRECT): single join on from_account_id, no duplicates
SELECT t.id, t.amount, t.type, t.category, t.status
FROM dbo.transactions t
INNER JOIN dbo.accounts a ON t.from_account_id = a.id
WHERE a.user_id = 'user_3'
ORDER BY t.created_at DESC
```
**Result**: Clean list, no duplicates, properly ordered.

---

### Demo 4: "How much did I spend on transportation?"

**Why it's hard**: Category values are case-sensitive in the database (`Transportation`, not `transportation`). The original config used `LOWER()` which works but adds unnecessary overhead. More critically, without data awareness the agent may query the wrong user.

#### ❌ Before (No Terminology Guidance)
The agent might:
- Use `LIKE '%transport%'` (matches unintended categories)
- Use `LOWER(t.category)` on every row (no index usage)
- Not know that some users have zero transportation transactions (returning NULL without explanation)

```sql
-- Possibly generated (INEFFICIENT + WRONG user context)
SELECT SUM(t.amount)
FROM dbo.transactions t
JOIN dbo.accounts a ON t.from_account_id = a.id OR t.to_account_id = a.id
WHERE a.user_id = 'user_1'
AND LOWER(t.category) LIKE '%transport%'
```
**Result**: NULL (user_1 has no transportation transactions) — confusing with no explanation.

#### ✅ After (With Terminology in DS Instructions)
DS instructions list exact category values: *"Transportation, Groceries, Housing, Food..."*
Few-shot #8 demonstrates with user_2 (who actually has transportation data).

```sql
-- Generated (CORRECT): exact case match, correct user
SELECT SUM(t.amount) AS total_transportation
FROM dbo.transactions t
INNER JOIN dbo.accounts a ON t.from_account_id = a.id
WHERE a.user_id = 'user_2' AND t.category = 'Transportation'
```
**Result**: $60.00 ✅

---

### Demo 5: "What's my income vs expenses?"

**Why it's hard**: This is a **composite question** requiring two separate aggregations. Without business rule context, the agent doesn't know how to classify income vs expenses.

#### ❌ Before (No Business Rules)
The agent has no concept of:
- Income = deposits + Salary/Income categories
- Expenses = payments only
- Transfers are neither income nor expense

```sql
-- Likely generated (WRONG): treats all transactions as one bucket
SELECT type, SUM(amount) as total
FROM dbo.transactions t
JOIN dbo.accounts a ON t.from_account_id = a.id
WHERE a.user_id = 'user_1'
GROUP BY type
```
**Result**: Shows payment/transfer/deposit totals — but "deposit" isn't the same as "income" (a deposit could be a refund), and the user wanted a clear income vs expense comparison.

#### ✅ After (With Business Rules in DS Instructions)
DS instructions define:
- *"Total spending = SUM(amount) WHERE type = 'payment'"*
- *"Total income = SUM(amount) WHERE type = 'deposit' OR category IN ('Income', 'Salary')"*

```sql
-- Generated (CORRECT): uses business rule definitions
SELECT 
    SUM(CASE WHEN t.type = 'payment' THEN t.amount ELSE 0 END) AS total_expenses,
    SUM(CASE WHEN t.type = 'deposit' OR t.category IN ('Income', 'Salary') 
        THEN t.amount ELSE 0 END) AS total_income
FROM dbo.transactions t
INNER JOIN dbo.accounts a ON t.from_account_id = a.id
WHERE a.user_id = 'user_1'
```
**Result**: Clear income vs expenses comparison with correct business logic.

---

## Configuration Best Practices Applied

Based on empirical research from the [DFA Skill Toolkit](https://github.com/microsoft/dfa-skill):

| Pattern | What We Did | Impact |
|---------|------------|--------|
| **P1: Structured Headers** | Used `## Section` headers in instructions | +7.8% accuracy |
| **P2: Join Path Annotations** | Explicit `transactions → accounts → users` paths | 0% → 100% on multi-table queries |
| **P3: Targeted Few-Shots** | 8 examples covering key patterns (not flooding) | Fixed known failure modes |
| **P4: Column Semantics** | Defined `from_account_id` vs `to_account_id` meaning | Eliminated ambiguous joins |
| **P8: Terminology** | Listed exact category values, account types | Correct case-sensitive matching |

### Anti-Patterns Avoided
| Anti-Pattern | Risk | How We Avoided It |
|---|---|---|
| **A1: Conflicting Instructions** | 100% → 0% regression | Agent instructions = routing/tone only; DS instructions = schema/business rules |
| **A3: Instruction Overflow** | Accuracy drops past ~4600 chars | Kept DS instructions under 3000 chars |
| **A5: Few-Shot Flooding** | Noise overwhelms signal | 8 examples (under recommended max), each covering a distinct pattern |

---

## Try These Questions in the App

Open http://localhost:5173 and try:

1. **"What are my account balances?"** → Should show per-account breakdown
2. **"What is my total spending by category?"** → Should show only payments, grouped by category
3. **"Show my recent transactions"** → Should list transactions without duplicates
4. **"How much did I spend on groceries?"** → Should return exact amount with correct category match
5. **"What's my total balance across all accounts?"** → Should return a single aggregated number

> 💡 **Tip**: The app automatically prefixes your question with `[user_id: ...]` so the Data Agent scopes results to your logged-in user.
