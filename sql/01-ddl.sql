-- ============================================================================
-- DDL: Tabelas de origem no Neon (Postgres)
-- Seed determinístico: seed=42
-- Volumes: customers=200 | accounts=240 | cards=320 | merchants=60 | transactions=2000
-- ============================================================================

CREATE TABLE IF NOT EXISTS customers (
  id            BIGSERIAL PRIMARY KEY,
  full_name     TEXT NOT NULL,          -- PII
  email         TEXT NOT NULL,          -- PII
  document      TEXT NOT NULL,          -- PII
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
  id            BIGSERIAL PRIMARY KEY,
  customer_id   BIGINT NOT NULL REFERENCES customers(id),
  status        TEXT NOT NULL,
  balance       NUMERIC(15,2) NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cards (
  id            BIGSERIAL PRIMARY KEY,
  account_id    BIGINT NOT NULL REFERENCES accounts(id),
  pan_masked    TEXT NOT NULL,          -- PCI
  status        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchants (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL,
  category      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
  id            BIGSERIAL PRIMARY KEY,
  card_id       BIGINT NOT NULL REFERENCES cards(id),
  account_id    BIGINT NOT NULL REFERENCES accounts(id),
  merchant_id   BIGINT NOT NULL REFERENCES merchants(id),
  amount        NUMERIC(15,2) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Necessário para o CDC enviar "before" completo no UPDATE/DELETE
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE cards        REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- Menor privilégio para o conector
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_user') THEN
    EXECUTE format('CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD %L',
                   current_setting('app.cdc_password', true));
  END IF;
END
$$;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;

-- Publicação para o CDC capturar apenas estas tabelas
DROP PUBLICATION IF EXISTS desafio_pub;
CREATE PUBLICATION desafio_pub FOR TABLE customers, accounts, cards, merchants, transactions;
