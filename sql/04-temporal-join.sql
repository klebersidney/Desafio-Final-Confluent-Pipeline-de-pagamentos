-- ============================================================================
-- Flink SQL — Temporal Join: Transações ⋈ Contas
-- Enriquece cada transação com status e saldo da conta no momento do evento
-- ============================================================================

SET 'sql.tables.scan.idle-timeout' = '5s';

CREATE TEMPORARY VIEW enriched_transactions AS
SELECT
  t.id            AS tx_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.created_at    AS tx_time,
  a.status        AS account_status,
  a.balance       AS account_balance,
  a.customer_id
FROM desafio_transactions AS t
JOIN desafio_accounts FOR SYSTEM_TIME AS OF t.created_at AS a
  ON t.account_id = a.id;
