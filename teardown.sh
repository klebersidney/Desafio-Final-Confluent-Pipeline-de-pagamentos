#!/usr/bin/env bash
# ============================================================================
# teardown.sh — Destrói todos os recursos do pipeline de pagamentos
#
# Derruba na ordem inversa: Flink → Connect → Topics → SAs → Cluster → Environment
# Verifica listas vazias ao final de cada etapa.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Carregar variáveis ──────────────────────────────────────────────────────
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "❌ Arquivo .env não encontrado."
  exit 1
fi
set -a; source "$SCRIPT_DIR/.env"; set +a

echo "🗑️  Iniciando teardown do pipeline de pagamentos..."
echo ""

confluent environment use "$ENVIRONMENT_ID"
confluent kafka cluster use "$CLUSTER_ID"

# ============================================================================
# 1. Flink Statements
# ============================================================================
echo "═══════════════════════════════════════════════════════════════"
echo "  1/6 — Removendo Flink statements..."
echo "═══════════════════════════════════════════════════════════════"

STATEMENTS=$(confluent flink statement list --environment "$ENVIRONMENT_ID" -o json 2>/dev/null | jq -r '.[].name // empty' 2>/dev/null || true)
if [[ -n "$STATEMENTS" ]]; then
  for stmt in $STATEMENTS; do
    echo "  ➜ Deletando statement: $stmt"
    confluent flink statement delete "$stmt" \
      --environment "$ENVIRONMENT_ID" --force 2>/dev/null || true
  done
else
  echo "  ✔ Nenhum statement encontrado"
fi

echo "  Verificando: confluent flink statement list"
confluent flink statement list --environment "$ENVIRONMENT_ID" 2>/dev/null || echo "  ✔ None found"

# ============================================================================
# 2. Flink Compute Pool
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  2/6 — Removendo Flink compute pool..."
echo "═══════════════════════════════════════════════════════════════"

POOLS=$(confluent flink compute-pool list --environment "$ENVIRONMENT_ID" -o json 2>/dev/null | jq -r '.[].id // empty' 2>/dev/null || true)
if [[ -n "$POOLS" ]]; then
  for pool in $POOLS; do
    echo "  ➜ Deletando compute pool: $pool"
    confluent flink compute-pool delete "$pool" \
      --environment "$ENVIRONMENT_ID" --force 2>/dev/null || true
  done
else
  echo "  ✔ Nenhum compute pool encontrado"
fi

# ============================================================================
# 3. Conectores
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  3/6 — Removendo conectores..."
echo "═══════════════════════════════════════════════════════════════"

CONNECTORS=$(confluent connect cluster list -o json 2>/dev/null | jq -r '.[].id // empty' 2>/dev/null || true)
if [[ -n "$CONNECTORS" ]]; then
  for conn in $CONNECTORS; do
    echo "  ➜ Deletando conector: $conn"
    confluent connect cluster delete "$conn" --force 2>/dev/null || true
  done
else
  echo "  ✔ Nenhum conector encontrado"
fi

echo "  Verificando: confluent connect cluster list"
confluent connect cluster list 2>/dev/null || echo "  ✔ None found"

# ============================================================================
# 4. Service Accounts e ACLs
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  4/6 — Removendo Service Accounts e ACLs..."
echo "═══════════════════════════════════════════════════════════════"

for SA_ID in "$SA_PRODUCER_ID" "$SA_CONSUMER_ID"; do
  if [[ -n "$SA_ID" ]]; then
    echo "  ➜ Removendo ACLs da SA $SA_ID..."
    confluent kafka acl delete --allow --service-account "$SA_ID" \
      --operations WRITE,DESCRIBE,CREATE,READ --topic "desafio" --prefix --force 2>/dev/null || true
    confluent kafka acl delete --allow --service-account "$SA_ID" \
      --operations READ --consumer-group "desafio" --prefix --force 2>/dev/null || true

    echo "  ➜ Deletando service account: $SA_ID"
    confluent iam service-account delete "$SA_ID" --force 2>/dev/null || true
  fi
done

# ============================================================================
# 5. Cluster
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  5/6 — Removendo cluster..."
echo "═══════════════════════════════════════════════════════════════"

echo "  ➜ Deletando cluster: $CLUSTER_ID"
confluent kafka cluster delete "$CLUSTER_ID" --force 2>/dev/null || true
echo "  ✔ Cluster deletado"

# ============================================================================
# 6. Environment
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  6/6 — Removendo environment..."
echo "═══════════════════════════════════════════════════════════════"

echo "  ➜ Deletando environment: $ENVIRONMENT_ID"
confluent environment delete "$ENVIRONMENT_ID" --force 2>/dev/null || true
echo "  ✔ Environment deletado"

# ============================================================================
# Verificação final
# ============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✅ TEARDOWN COMPLETO"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "  Verificações finais:"
echo "    confluent kafka cluster list        → Nenhum cluster restante"
echo "    confluent environment list          → Sem 'desafio-final'"
echo "    confluent connect cluster list      → None found"
echo "    confluent flink statement list      → None found"
echo ""
echo "  ⚠️  Lembre-se de limpar as tabelas no Neon se desejar:"
echo "    DROP TABLE transactions, cards, accounts, merchants, customers CASCADE;"
echo "    DROP PUBLICATION desafio_pub;"
echo "    DROP ROLE cdc_user;"
echo ""
