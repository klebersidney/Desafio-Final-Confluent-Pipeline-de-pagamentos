-- ============================================================================
-- Flink SQL — Tabelas-fonte (CDC topics)
-- Configuração: sql.tables.scan.idle-timeout = 5s
-- ============================================================================

SET 'sql.tables.scan.idle-timeout' = '5s';

-- ─── Tabela de Transações (do tópico CDC) ────────────────────────────────────
CREATE TABLE desafio_transactions (
  `id`          BIGINT,
  `card_id`     BIGINT,
  `account_id`  BIGINT,
  `merchant_id` BIGINT,
  `amount`      DECIMAL(15,2),
  `created_at`  TIMESTAMP(3),
  WATERMARK FOR `created_at` AS `created_at` - INTERVAL '5' SECOND,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'desafio.public.transactions',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent'
);

-- ─── Tabela de Contas (do tópico CDC) ────────────────────────────────────────
CREATE TABLE desafio_accounts (
  `id`          BIGINT,
  `customer_id` BIGINT,
  `status`      STRING,
  `balance`     DECIMAL(15,2),
  `updated_at`  TIMESTAMP(3),
  WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '5' SECOND,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'desafio.public.accounts',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent'
);

-- ─── Tabela de Alertas de Fraude (sink) ──────────────────────────────────────
CREATE TABLE desafio_fraud_detected (
  `alert_id`        STRING,
  `rule`            STRING,
  `card_id`         BIGINT,
  `account_id`      BIGINT,
  `tx_id_1`         BIGINT,
  `tx_id_2`         BIGINT,
  `tx_id_3`         BIGINT,
  `total_amount`    DECIMAL(15,2),
  `window_start`    TIMESTAMP(3),
  `window_end`      TIMESTAMP(3),
  `detected_at`     TIMESTAMP(3),
  PRIMARY KEY (`alert_id`) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'desafio.fraud.detected',
  'value.format' = 'avro-confluent'
);
