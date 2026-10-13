#!/usr/bin/env bash
# ============================================================================
# setup.sh — Provisiona o pipeline de pagamentos completo no Confluent Cloud
#
# Pré-requisitos:
#   1. CLI confluent instalada e autenticada (confluent login)
#   2. psql disponível no PATH
#   3. Arquivo .env preenchido (copiar de .env.example)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Carregar variáveis ──────────────────────────────────────────────────────
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "❌ Arquivo .env não encontrado. Copie .env.example para .env e preencha."
  exit 1
fi
set -a; source "$SCRIPT_DIR/.env"; set +a

echo "🚀 Iniciando setup do pipeline de pagamentos..."

# ============================================================================
# CAMADA 1 — Fundação
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CAMADA 1 — Fundação (Environment, Cluster, Service Accounts)"
echo "═══════════════════════════════════════════════════════════════"

# ─── Environment ─────────────────────────────────────────────────────────────
echo "➜ Criando environment desafio-final..."
ENV_OUTPUT=$(confluent environment create desafio-final -o json)
ENVIRONMENT_ID=$(echo "$ENV_OUTPUT" | jq -r '.id')
echo "  ✔ Environment: $ENVIRONMENT_ID"

confluent environment use "$ENVIRONMENT_ID"

# ─── Cluster ─────────────────────────────────────────────────────────────────
echo "➜ Criando cluster Basic desafio-basic (aws/us-east-1)..."
CLUSTER_OUTPUT=$(confluent kafka cluster create desafio-basic \
  --cloud aws \
  --region us-east-1 \
  --type basic \
  -o json)
CLUSTER_ID=$(echo "$CLUSTER_OUTPUT" | jq -r '.id')
BOOTSTRAP_SERVER=$(echo "$CLUSTER_OUTPUT" | jq -r '.endpoint' | sed 's|SASL_SSL://||')
echo "  ✔ Cluster: $CLUSTER_ID"
echo "  ✔ Bootstrap: $BOOTSTRAP_SERVER"

confluent kafka cluster use "$CLUSTER_ID"

# Aguardar cluster ficar pronto
echo "➜ Aguardando cluster ficar READY..."
while true; do
  STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.status')
  if [[ "$STATUS" == "UP" ]]; then break; fi
  echo "  ⏳ Status: $STATUS, aguardando..."
  sleep 10
done
echo "  ✔ Cluster pronto!"

# ─── Service Accounts ────────────────────────────────────────────────────────
echo "➜ Criando service account desafio-producer..."
SA_PRODUCER_OUTPUT=$(confluent iam service-account create desafio-producer \
  --description "Produtor do pipeline de pagamentos" -o json)
SA_PRODUCER_ID=$(echo "$SA_PRODUCER_OUTPUT" | jq -r '.id')

echo "➜ Criando API Key para desafio-producer..."
PRODUCER_KEY_OUTPUT=$(confluent api-key create \
  --service-account "$SA_PRODUCER_ID" \
  --resource "$CLUSTER_ID" -o json)
SA_PRODUCER_API_KEY=$(echo "$PRODUCER_KEY_OUTPUT" | jq -r '.api_key // .key')
SA_PRODUCER_API_SECRET=$(echo "$PRODUCER_KEY_OUTPUT" | jq -r '.api_secret // .secret')

echo "➜ Configurando ACLs para desafio-producer (WRITE em desafio-*)..."
confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" \
  --operations WRITE,DESCRIBE --topic "desafio" --prefix
confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" \
  --operations CREATE --topic "desafio" --prefix

echo "  ✔ desafio-producer: $SA_PRODUCER_ID"

echo "➜ Criando service account desafio-consumer..."
SA_CONSUMER_OUTPUT=$(confluent iam service-account create desafio-consumer \
  --description "Consumidor do pipeline de pagamentos" -o json)
SA_CONSUMER_ID=$(echo "$SA_CONSUMER_OUTPUT" | jq -r '.id')

echo "➜ Criando API Key para desafio-consumer..."
CONSUMER_KEY_OUTPUT=$(confluent api-key create \
  --service-account "$SA_CONSUMER_ID" \
  --resource "$CLUSTER_ID" -o json)
SA_CONSUMER_API_KEY=$(echo "$CONSUMER_KEY_OUTPUT" | jq -r '.api_key // .key')
SA_CONSUMER_API_SECRET=$(echo "$CONSUMER_KEY_OUTPUT" | jq -r '.api_secret // .secret')

echo "➜ Configurando ACLs para desafio-consumer (READ em desafio-* e grupo desafio-*)..."
confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" \
  --operations READ,DESCRIBE --topic "desafio" --prefix
confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" \
  --operations READ --consumer-group "desafio" --prefix

echo "  ✔ desafio-consumer: $SA_CONSUMER_ID"

# ─── Schema Registry ─────────────────────────────────────────────────────────
echo "➜ Habilitando Schema Registry..."
SR_OUTPUT=$(confluent schema-registry cluster describe -o json 2>/dev/null || \
  confluent schema-registry cluster enable --cloud aws --geo us -o json)
SR_URL=$(echo "$SR_OUTPUT" | jq -r '.endpoint_url // .endpoint')
echo "  ✔ Schema Registry: $SR_URL"

echo "➜ Criando API Key para Schema Registry..."
SR_KEY_OUTPUT=$(confluent api-key create --resource "$SR_URL" -o json 2>/dev/null || \
  confluent api-key create --resource "$(echo "$SR_OUTPUT" | jq -r '.id // .cluster_id')" -o json)
SR_API_KEY=$(echo "$SR_KEY_OUTPUT" | jq -r '.api_key // .key')
SR_API_SECRET=$(echo "$SR_KEY_OUTPUT" | jq -r '.api_secret // .secret')

# ─── Salvar IDs no .env ─────────────────────────────────────────────────────
echo "➜ Atualizando .env com IDs provisionados..."
sed -i "s|^ENVIRONMENT_ID=.*|ENVIRONMENT_ID=$ENVIRONMENT_ID|" "$SCRIPT_DIR/.env"
sed -i "s|^CLUSTER_ID=.*|CLUSTER_ID=$CLUSTER_ID|" "$SCRIPT_DIR/.env"
sed -i "s|^BOOTSTRAP_SERVER=.*|BOOTSTRAP_SERVER=$BOOTSTRAP_SERVER|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_PRODUCER_ID=.*|SA_PRODUCER_ID=$SA_PRODUCER_ID|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_PRODUCER_API_KEY=.*|SA_PRODUCER_API_KEY=$SA_PRODUCER_API_KEY|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_PRODUCER_API_SECRET=.*|SA_PRODUCER_API_SECRET=$SA_PRODUCER_API_SECRET|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_CONSUMER_ID=.*|SA_CONSUMER_ID=$SA_CONSUMER_ID|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_CONSUMER_API_KEY=.*|SA_CONSUMER_API_KEY=$SA_CONSUMER_API_KEY|" "$SCRIPT_DIR/.env"
sed -i "s|^SA_CONSUMER_API_SECRET=.*|SA_CONSUMER_API_SECRET=$SA_CONSUMER_API_SECRET|" "$SCRIPT_DIR/.env"
sed -i "s|^SR_URL=.*|SR_URL=$SR_URL|" "$SCRIPT_DIR/.env"
sed -i "s|^SR_API_KEY=.*|SR_API_KEY=$SR_API_KEY|" "$SCRIPT_DIR/.env"
sed -i "s|^SR_API_SECRET=.*|SR_API_SECRET=$SR_API_SECRET|" "$SCRIPT_DIR/.env"

echo "  ✔ .env atualizado"

# ============================================================================
# CAMADA 2 — Contrato de Dados
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CAMADA 2 — Contrato de Dados (Schema Registry)"
echo "═══════════════════════════════════════════════════════════════"

SCHEMAS=("customer" "account" "card" "merchant" "transaction" "fraud_alert")
SUBJECTS=(
  "desafio.public.customers-value"
  "desafio.public.accounts-value"
  "desafio.public.cards-value"
  "desafio.public.merchants-value"
  "desafio.public.transactions-value"
  "desafio.fraud.detected-value"
)

for i in "${!SCHEMAS[@]}"; do
  SCHEMA_FILE="$SCRIPT_DIR/schemas/${SCHEMAS[$i]}.avsc"
  SUBJECT="${SUBJECTS[$i]}"
  echo "➜ Registrando schema ${SCHEMAS[$i]} no subject $SUBJECT..."
  confluent schema-registry schema create \
    --subject "$SUBJECT" \
    --schema "$SCHEMA_FILE" \
    --type avro
  echo "  ✔ Schema registrado"
done

# ─── Teste de compatibilidade BACKWARD ───────────────────────────────────────
echo ""
echo "➜ Teste de compatibilidade: campo novo com default (deve ACEITAR)..."
confluent schema-registry schema test-compatibility \
  --schema "$SCRIPT_DIR/schemas/customer_v2_add_field.avsc" \
  --subject "desafio.public.customers-value" \
  --type avro && echo "  ✔ Compatível (esperado)" || echo "  ✖ Incompatível (inesperado!)"

echo "➜ Teste de compatibilidade: remover campo obrigatório (deve REJEITAR)..."
confluent schema-registry schema test-compatibility \
  --schema "$SCRIPT_DIR/schemas/customer_v2_remove_field.avsc" \
  --subject "desafio.public.customers-value" \
  --type avro && echo "  ✖ Compatível (inesperado!)" || echo "  ✔ Incompatível (esperado)"

# ============================================================================
# CAMADA 3 — Ingestão (Postgres + CDC)
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CAMADA 3 — Ingestão (Postgres seed + CDC connector)"
echo "═══════════════════════════════════════════════════════════════"

# ─── Seed no Postgres ────────────────────────────────────────────────────────
echo "➜ Aplicando DDL no Postgres (Neon)..."
PGPASSWORD="$PG_PASSWORD" psql \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
  -v CDC_PASSWORD="$CDC_PASSWORD" \
  -f "$SCRIPT_DIR/sql/01-ddl.sql"
echo "  ✔ DDL aplicado"

echo "➜ Carregando seed determinístico (seed=42)..."
PGPASSWORD="$PG_PASSWORD" psql \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
  -f "$SCRIPT_DIR/sql/02-seed.sql"
echo "  ✔ Seed carregado: 200 customers, 240 accounts, 320 cards, 60 merchants, 2000 transactions"

# ─── CDC Connector ───────────────────────────────────────────────────────────
echo "➜ Criando conector CDC PostgresCdcSourceV2..."
confluent connect cluster create \
  --config-file "$SCRIPT_DIR/connectors/cdc.json" \
  --cluster "$CLUSTER_ID" \
  --environment "$ENVIRONMENT_ID"
echo "  ✔ Conector CDC criado"

echo "➜ Aguardando conector ficar RUNNING..."
sleep 30
echo "  ✔ Conector em execução (verifique com: confluent connect cluster list)"

# ============================================================================
# CAMADA 4 — Processamento (Flink SQL)
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CAMADA 4 — Processamento (Flink SQL)"
echo "═══════════════════════════════════════════════════════════════"

# ─── Compute Pool Flink ──────────────────────────────────────────────────────
echo "➜ Criando Flink compute pool..."
FLINK_POOL_OUTPUT=$(confluent flink compute-pool create desafio-flink \
  --cloud aws \
  --region us-east-1 \
  --max-cfu 10 \
  -o json)
FLINK_POOL_ID=$(echo "$FLINK_POOL_OUTPUT" | jq -r '.id')
echo "  ✔ Flink compute pool: $FLINK_POOL_ID"

echo "➜ Aguardando Flink compute pool ficar READY..."
sleep 30

# ─── Submeter statements Flink ───────────────────────────────────────────────
echo "➜ Criando tabelas Flink..."
confluent flink statement create setup-tables \
  --sql "$(cat "$SCRIPT_DIR/sql/03-flink-tables.sql")" \
  --compute-pool "$FLINK_POOL_ID" \
  --database "$ENVIRONMENT_ID" \
  --environment "$ENVIRONMENT_ID"
echo "  ✔ Tabelas Flink criadas"

sleep 10

echo "➜ Criando temporal join..."
confluent flink statement create setup-temporal-join \
  --sql "$(cat "$SCRIPT_DIR/sql/04-temporal-join.sql")" \
  --compute-pool "$FLINK_POOL_ID" \
  --database "$ENVIRONMENT_ID" \
  --environment "$ENVIRONMENT_ID"
echo "  ✔ Temporal join criado"

sleep 10

echo "➜ Ativando detecção de fraude (RULE_VELOCITY)..."
confluent flink statement create fraud-detection \
  --sql "$(cat "$SCRIPT_DIR/sql/05-fraud-detection.sql")" \
  --compute-pool "$FLINK_POOL_ID" \
  --database "$ENVIRONMENT_ID" \
  --environment "$ENVIRONMENT_ID"
echo "  ✔ RULE_VELOCITY ativa"

# ============================================================================
# CAMADA 5 — Operação (Evidências)
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  CAMADA 5 — Operação (Métricas, Custos)"
echo "═══════════════════════════════════════════════════════════════"

echo "➜ Coletando métricas de observabilidade..."
START_ISO=$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)
END_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

curl -s -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query" \
  -H "Content-Type: application/json" \
  -d "{
    \"aggregations\": [{\"metric\": \"io.confluent.kafka.server/received_bytes\", \"agg\": \"MAX\"}],
    \"filter\": {\"field\": \"resource.kafka.id\", \"op\": \"EQ\", \"value\": \"$CLUSTER_ID\"},
    \"granularity\": \"PT1M\",
    \"intervals\": [\"$START_ISO/$END_ISO\"]
  }" | jq . > "$SCRIPT_DIR/evidencias/04-metrics-api-live.json" 2>/dev/null || true

echo "  ✔ Métricas salvas em evidencias/04-metrics-api-live.json"

echo "➜ Coletando billing cost..."
TODAY=$(date +%Y-%m-%d)
confluent billing cost list --start-date "$TODAY" --end-date "$TODAY" \
  -o json > "$SCRIPT_DIR/evidencias/05-billing-cost-live.json" 2>/dev/null || true
echo "  ✔ Custos salvos em evidencias/05-billing-cost-live.json"

# ============================================================================
# RESUMO
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✅ SETUP COMPLETO"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "  Environment:    $ENVIRONMENT_ID"
echo "  Cluster:        $CLUSTER_ID"
echo "  Bootstrap:      $BOOTSTRAP_SERVER"
echo "  Schema Registry: $SR_URL"
echo "  Flink Pool:     $FLINK_POOL_ID"
echo "  SA Producer:    $SA_PRODUCER_ID"
echo "  SA Consumer:    $SA_CONSUMER_ID"
echo ""
echo "  Próximos passos:"
echo "    1. Verifique os tópicos:     confluent kafka topic list"
echo "    2. Verifique o conector:     confluent connect cluster list"
echo "    3. Verifique os statements:  confluent flink statement list"
echo "    4. Consuma alertas de fraude: confluent kafka topic consume desafio.fraud.detected --from-beginning"
echo ""
