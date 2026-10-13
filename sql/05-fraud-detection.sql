-- ============================================================================
-- Flink SQL — RULE_VELOCITY: Detecção de fraude via MATCH_RECOGNIZE
-- Regra: ≥ 3 transações no mesmo card_id em < 60 segundos
-- Resultado emitido no tópico desafio.fraud.detected
-- ============================================================================

SET 'sql.tables.scan.idle-timeout' = '5s';

INSERT INTO desafio_fraud_detected
SELECT
  alert_id,
  rule_name       AS `rule`,
  card_id,
  account_id,
  tx_id_1,
  tx_id_2,
  tx_id_3,
  total_amount,
  window_start,
  window_end,
  detected_at
FROM enriched_transactions
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY tx_time
  MEASURES
    CONCAT('FRAUD-', CAST(FIRST(T.tx_id) AS STRING), '-', CAST(LAST(T.tx_id) AS STRING))
                                              AS alert_id,
    'RULE_VELOCITY'                           AS rule_name,
    FIRST(T.account_id)                       AS account_id,
    FIRST(T.tx_id)                            AS tx_id_1,
    NTH(T.tx_id, 2)                           AS tx_id_2,
    LAST(T.tx_id)                             AS tx_id_3,
    FIRST(T.amount) + NTH(T.amount, 2) + LAST(T.amount)
                                              AS total_amount,
    FIRST(T.tx_time)                          AS window_start,
    LAST(T.tx_time)                           AS window_end,
    CURRENT_TIMESTAMP                         AS detected_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE
) AS fraud;
