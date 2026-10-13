-- ============================================================================
-- Seed determinístico (seed=42)
-- Gera dados reprodutíveis usando setseed() do Postgres
-- Volumes: customers=200 | accounts=240 | cards=320 | merchants=60 | transactions=2000
-- ============================================================================

SELECT setseed(0.42);

-- ─── Customers (200) ─────────────────────────────────────────────────────────
INSERT INTO customers (full_name, email, document, created_at)
SELECT
  'Customer_' || i                                                       AS full_name,
  'customer_' || i || '@mail.com'                                        AS email,
  lpad(floor(random() * 99999999999)::bigint::text, 11, '0')            AS document,
  now() - (random() * interval '365 days')                               AS created_at
FROM generate_series(1, 200) AS i;

-- ─── Accounts (240) ──────────────────────────────────────────────────────────
INSERT INTO accounts (customer_id, status, balance, updated_at)
SELECT
  (floor(random() * 200) + 1)::bigint                                    AS customer_id,
  CASE WHEN random() < 0.9 THEN 'ACTIVE' ELSE 'BLOCKED' END             AS status,
  round((random() * 50000)::numeric, 2)                                  AS balance,
  now() - (random() * interval '180 days')                               AS updated_at
FROM generate_series(1, 240) AS i;

-- ─── Cards (320) ─────────────────────────────────────────────────────────────
INSERT INTO cards (account_id, pan_masked, status)
SELECT
  (floor(random() * 240) + 1)::bigint                                    AS account_id,
  '****-****-****-' || lpad(floor(random() * 9999)::int::text, 4, '0')  AS pan_masked,
  CASE
    WHEN random() < 0.8 THEN 'ACTIVE'
    WHEN random() < 0.95 THEN 'BLOCKED'
    ELSE 'CANCELLED'
  END                                                                     AS status
FROM generate_series(1, 320) AS i;

-- ─── Merchants (60) ─────────────────────────────────────────────────────────
INSERT INTO merchants (name, category)
SELECT
  'Merchant_' || i                                                        AS name,
  (ARRAY['FOOD','ELECTRONICS','TRAVEL','HEALTH','ENTERTAINMENT','SERVICES',
         'CLOTHING','EDUCATION','FUEL','GROCERY'])[floor(random()*10+1)::int] AS category
FROM generate_series(1, 60) AS i;

-- ─── Transactions (2000) ─────────────────────────────────────────────────────
-- Inclui burst de 3 transações no mesmo card_id em < 60s para disparar RULE_VELOCITY
INSERT INTO transactions (card_id, account_id, merchant_id, amount, created_at)
SELECT
  (floor(random() * 320) + 1)::bigint                                    AS card_id,
  (floor(random() * 240) + 1)::bigint                                    AS account_id,
  (floor(random() * 60) + 1)::bigint                                     AS merchant_id,
  round((random() * 5000 + 1)::numeric, 2)                               AS amount,
  now() - (random() * interval '30 days')                                 AS created_at
FROM generate_series(1, 1994) AS i;

-- Burst fraud: 3 transações no card_id=1, mesmo minuto, para disparar RULE_VELOCITY
INSERT INTO transactions (card_id, account_id, merchant_id, amount, created_at) VALUES
  (1, 1, 1, 4999.99, now() - interval '10 seconds'),
  (1, 1, 2, 3500.00, now() - interval '7 seconds'),
  (1, 1, 3, 2750.50, now() - interval '3 seconds');

-- Burst fraud: 3 transações no card_id=2, mesmo minuto (segundo caso)
INSERT INTO transactions (card_id, account_id, merchant_id, amount, created_at) VALUES
  (2, 2, 4, 1200.00, now() - interval '8 seconds'),
  (2, 2, 5, 1800.00, now() - interval '5 seconds'),
  (2, 2, 6, 2200.00, now() - interval '2 seconds');

-- ─── Gerar eventos UPDATE e DELETE para evidenciar op=u e op=d ──────────────
-- UPDATE: muda o saldo da account_id=1 → gera envelope com before+after
UPDATE accounts SET balance = balance + 100.00, updated_at = now() WHERE id = 1;

-- DELETE: remove a última transação → gera envelope com after=null + tombstone
DELETE FROM transactions WHERE id = (SELECT max(id) FROM transactions);
