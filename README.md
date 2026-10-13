# Pipeline de Pagamentos em Tempo Real — Desafio Final Confluent

Pipeline CDC end-to-end: **Neon (Postgres) → Debezium CDC → Kafka (Avro) → Flink SQL (enriquecimento + detecção de fraude) → Consumo**, tudo no Confluent Cloud.

## Arquitetura

```
┌──────────┐   CDC (Debezium)   ┌─────────────────┐   Avro   ┌───────────────────────┐
│  Neon    │──────────────────▶│  Kafka Topics   │────────▶│  Flink SQL            │
│ Postgres │  PostgresCdcSourceV2│  desafio.*      │         │  Temporal Join        │
│          │  heartbeat=60s     │                 │         │  MATCH_RECOGNIZE      │
└──────────┘                    └─────────────────┘         │  (RULE_VELOCITY)      │
                                                            └───────┬───────────────┘
                                                                    │
                                                            ┌───────▼───────────────┐
                                                            │ desafio.fraud.detected│
                                                            │ (alertas de fraude)   │
                                                            └───────────────────────┘
```

## Estrutura do Repositório

```
.
├── .env.example              # Template de variáveis (sem secrets)
├── .gitignore                # Exclui .env, *.pem, *.key, *.secret
├── setup.sh                  # Provisiona todo o pipeline do zero
├── teardown.sh               # Derruba tudo com verificação de listas vazias
├── schemas/
│   ├── customer.avsc         # PII: full_name, email, document
│   ├── account.avsc          # money: bytes + decimal(15,2)
│   ├── card.avsc             # PCI: pan_masked
│   ├── merchant.avsc
│   ├── transaction.avsc      # money: bytes + decimal(15,2)
│   ├── fraud_alert.avsc      # Alerta emitido pelo Flink
│   ├── customer_v2_add_field.avsc     # Teste BACKWARD: aceita campo com default
│   └── customer_v2_remove_field.avsc  # Teste BACKWARD: rejeita remoção
├── connectors/
│   └── cdc.json              # PostgresCdcSourceV2 (secrets via env vars)
├── sql/
│   ├── 01-ddl.sql            # DDL + REPLICA IDENTITY FULL + cdc_user
│   ├── 02-seed.sql           # Seed determinístico (seed=42)
│   ├── 03-flink-tables.sql   # Tabelas Flink (source + sink)
│   ├── 04-temporal-join.sql  # Transações ⋈ Contas (temporal join)
│   └── 05-fraud-detection.sql # RULE_VELOCITY via MATCH_RECOGNIZE
├── evidencias/                       # Gerado pela execução real do pipeline
│   ├── 01-envelopes-cdc.json         # (gerado) op=c, op=u, op=d + tombstone
│   ├── 02-fraud-alert.json           # (gerado) Alerta RULE_VELOCITY
│   ├── 03-schema-compatibility.json  # (gerado) Testes BACKWARD
│   ├── 04-metrics-api.json           # (gerado) received_bytes + consumer lag
│   └── 05-billing-cost.json          # (gerado) Custos + alavancas
└── README.md
```

---

## Camada 1 — Fundação

**O que faz:** Provisiona a infraestrutura base no Confluent Cloud.

| Recurso | Configuração | Porquê |
|---|---|---|
| Environment `desafio-final` | — | Isolamento lógico de todos os recursos do desafio |
| Cluster `desafio-basic` | Basic, aws/us-east-1 | Menor custo para PoC; região próxima ao Neon |
| SA `desafio-producer` | WRITE + CREATE em `desafio-*` | Menor privilégio: só escreve nos tópicos do pipeline |
| SA `desafio-consumer` | READ em `desafio-*` + grupo `desafio-*` | Menor privilégio: só lê tópicos e faz parte do consumer group |
| Conta pessoal | Apenas em `setup.sh` / `teardown.sh` | Operações admin (criar/destruir) não ficam em SAs de runtime |

**Evidência (comandos para verificar após execução):**
```bash
confluent environment list
confluent kafka cluster list
confluent iam service-account list
confluent kafka acl list
```

---

## Camada 2 — Contrato de Dados

**O que faz:** Registra os schemas Avro no Schema Registry com compatibilidade BACKWARD e valida evolução.

| Decisão | Porquê |
|---|---|
| Avro (não JSON/Protobuf) | Evolução de schema com compatibilidade; suporte nativo no Confluent |
| `bytes` + `decimal(15,2)` para dinheiro | Precisão financeira sem arredondamento de floating-point |
| Tags `PII` / `PCI` | Governança: identificar campos sensíveis para mascaramento/auditoria |
| Compatibilidade BACKWARD | Consumidores antigos continuam lendo mensagens com schema novo |

**Evidência (executar após registrar os schemas):**
```bash
# Deve aceitar: campo novo com default
confluent schema-registry schema test-compatibility \
  --schema schemas/customer_v2_add_field.avsc \
  --subject desafio.public.customers-value --type avro

# Deve rejeitar: remoção de campo obrigatório
confluent schema-registry schema test-compatibility \
  --schema schemas/customer_v2_remove_field.avsc \
  --subject desafio.public.customers-value --type avro
```
Salve as saídas em `evidencias/03-schema-compatibility.json`.

---

## Camada 3 — Ingestão (CDC)

**O que faz:** Carrega seed determinístico no Neon e captura mudanças via CDC Debezium.

| Decisão | Porquê |
|---|---|
| `setseed(0.42)` | Reprodutibilidade: mesma execução gera os mesmos dados |
| `REPLICA IDENTITY FULL` | CDC envia `before` completo em UPDATE/DELETE (necessário para temporal join) |
| `cdc_user` com `REPLICATION` + `SELECT` | Menor privilégio: sem escrita na origem |
| `heartbeat.interval.ms=60000` | Mantém o slot de replicação ativo e previne WAL bloat |
| Burst de fraude no seed | 3 transações no mesmo `card_id` em <60s para disparar RULE_VELOCITY |

**Volumes do seed:**

| Tabela | Registros |
|---|---|
| customers | 200 |
| accounts | 240 |
| cards | 320 |
| merchants | 60 |
| transactions | 2000 |

**Evidência (capturar após o conector rodar):**
```bash
# Consumir eventos CDC e salvar os 3 tipos de envelope (op=c, op=u, op=d + tombstone)
confluent kafka topic consume desafio.public.customers --from-beginning --value-format avro
```
Salve em `evidencias/01-envelopes-cdc.json` os envelopes com:
- `op=c` — Snapshot inicial (INSERT): `before=null`, `after` com dados completos
- `op=u` — UPDATE no saldo: `before` e `after` preenchidos (REPLICA IDENTITY FULL)
- `op=d` — DELETE da última transação: `after=null` + tombstone separado (`value=null`)

---

## Camada 4 — Processamento (Flink SQL)

**O que faz:** Enriquece transações e detecta fraude em tempo real.

### Temporal Join (Transações ⋈ Contas)

Enriquece cada transação com o estado da conta **no momento do evento** (não o estado atual).

```sql
SELECT t.*, a.status AS account_status, a.balance AS account_balance
FROM desafio_transactions AS t
JOIN desafio_accounts FOR SYSTEM_TIME AS OF t.created_at AS a
  ON t.account_id = a.id;
```

| Decisão | Porquê |
|---|---|
| `FOR SYSTEM_TIME AS OF` | Garantia temporal: usa o saldo vigente no instante da transação |
| `PRIMARY KEY NOT ENFORCED` | Necessário para temporal join no Flink; PK lógica vinda do CDC |
| `sql.tables.scan.idle-timeout=5s` | Libera recursos quando não há novos dados, reduz custo de CFU |

### Detecção de Fraude — RULE_VELOCITY

```sql
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY tx_time
  MEASURES ...
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
)
```

| Decisão | Porquê |
|---|---|
| `MATCH_RECOGNIZE` | Detecção de padrões complexos em streams, nativo do Flink SQL |
| `PARTITION BY card_id` | Cada cartão avaliado independentemente |
| `T{3,}?` (reluctant) | Detecta o menor grupo de ≥3 transações, sem esperar mais do que o necessário |
| `WITHIN INTERVAL '60' SECOND` | Janela de 60 segundos para velocidade suspeita |

**Evidência (capturar após Flink processar):**
```bash
confluent kafka topic consume desafio.fraud.detected --from-beginning --value-format avro
```
Salve em `evidencias/02-fraud-alert.json` o alerta com as 3 transações do mesmo `card_id`.

---

## Camada 5 — Operação

### 5.1 Observabilidade

Métricas a coletar via Metrics API (`/v2/metrics/cloud/query`):

| Métrica | Agregação | Descrição |
|---|---|---|
| `io.confluent.kafka.server/received_bytes` | MAX, 1min | Pico de ingestão (bytes recebidos pelo cluster) |
| `io.confluent.kafka.server/consumer_lag_offsets` | MAX, por grupo | Lag de cada consumer group |

**Evidência (executar após pipeline rodar):**
```bash
# Pico de received_bytes
curl -s -u $CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/query' \
  -H 'Content-Type: application/json' \
  -d '{"aggregations":[{"metric":"io.confluent.kafka.server/received_bytes","agg":"MAX"}],"filter":{"field":"resource.kafka.id","op":"EQ","value":"'$CLUSTER_ID'"},"granularity":"PT1M","intervals":["'$START_ISO'/'$END_ISO'"]}'

# Consumer lag por grupo
curl -s -u $CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET \
  'https://api.telemetry.confluent.cloud/v2/metrics/cloud/query' \
  -H 'Content-Type: application/json' \
  -d '{"aggregations":[{"metric":"io.confluent.kafka.server/consumer_lag_offsets","agg":"MAX"}],"filter":{"field":"resource.kafka.id","op":"EQ","value":"'$CLUSTER_ID'"},"group_by":["metric.consumer_group_id"],"granularity":"PT1M","intervals":["'$START_ISO'/'$END_ISO'"]}'
```
Salve as respostas em `evidencias/04-metrics-api.json`.

### 5.2 Segurança

| Service Account | ACLs | Porquê |
|---|---|---|
| `desafio-producer` | `WRITE`, `DESCRIBE`, `CREATE` em topic `desafio-*` | Produz mensagens e cria tópicos automaticamente (CDC) |
| `desafio-consumer` | `READ`, `DESCRIBE` em topic `desafio-*`; `READ` em group `desafio-*` | Consome de qualquer tópico do pipeline e faz commit de offsets |
| Conta pessoal | Admin (apenas `setup.sh` / `teardown.sh`) | Operações de ciclo de vida não devem ficar em SAs de runtime |

**Princípio:** menor privilégio — cada SA tem apenas as permissões estritamente necessárias para sua função.

### 5.3 Custos

**Evidência (executar após pipeline rodar):**
```bash
confluent billing cost list --start-date <DATA_INICIO> --end-date <DATA_FIM> -o json > evidencias/05-billing-cost.json
```

**Alavancas de redução de custo:**

1. **Reduzir retenção dos tópicos CDC** de 7 dias para 1 dia — dados de CDC são efêmeros e já foram processados pelo Flink; reduz storage em ~85%.
2. **Usar compactação** (`cleanup.policy=compact`) nos tópicos de entidades (accounts, customers) — mantém apenas o último estado de cada chave, reduzindo volume armazenado e tráfego de rede na re-leitura.

### 5.4 Confiabilidade

| Trecho | Garantia | Mecanismo |
|---|---|---|
| Postgres → CDC Connector | **At-least-once** | Slot de replicação lógica do Postgres preserva WAL até confirmação; heartbeat evita WAL bloat |
| CDC Connector → Kafka | **At-least-once** | Conector confirma offset somente após gravação no Kafka; re-delivery possível em falha |
| Kafka (tópicos) | **Durável** | Replicação gerenciada pelo Confluent Cloud (RF=3 nos clusters Basic) |
| Kafka → Flink SQL | **Exactly-once** | Checkpointing do Flink + consumer offsets transacionais |
| Flink SQL → tópico sink | **At-least-once** | Sem transações de 2PC no sink; duplicatas possíveis no `desafio.fraud.detected` |
| Tópico → Consumer final | **At-least-once** | Consumer gerencia commit de offsets; idempotência é responsabilidade do código consumidor |

**Elo fraco:** Consumer final → sem idempotência nativa. Se o consumer falhar após processar mas antes de commitar o offset, a mensagem será re-entregue. **Efeito:** alerta de fraude pode ser processado duas vezes. **Mitigação:** idempotência no código do consumidor (deduplicação por `alert_id`).

---

## Como Subir do Zero

### Pré-requisitos

1. [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) instalada
2. `psql` no PATH
3. `jq` e `curl` instalados
4. Conta no Confluent Cloud autenticada (`confluent login`)
5. Banco Neon (Postgres) provisionado

### Passos

```bash
# 1. Clone o repositório
git clone https://github.com/klebersidney/Desafio-Final-Confluent-Pipeline-de-pagamentos.git
cd Desafio-Final-Confluent-Pipeline-de-pagamentos

# 2. Copie e preencha o .env
cp .env.example .env
# Edite .env com suas credenciais do Confluent Cloud e Neon

# 3. Execute o setup
chmod +x setup.sh teardown.sh
./setup.sh

# 4. Acompanhe os alertas de fraude
confluent kafka topic consume desafio.fraud.detected --from-beginning
```

O `setup.sh` executa todas as 5 camadas automaticamente:
1. Cria environment, cluster, service accounts e ACLs
2. Registra schemas e testa compatibilidade BACKWARD
3. Aplica DDL + seed no Postgres e inicia o conector CDC
4. Cria compute pool Flink e submete as queries SQL
5. Coleta métricas e custos

---

## Como Derrubar

```bash
./teardown.sh
```

O `teardown.sh` destrói na ordem inversa:
1. Remove Flink statements e compute pool
2. Remove conectores
3. Remove service accounts e ACLs
4. Deleta cluster
5. Deleta environment

Ao final, verifica listas vazias:
```bash
confluent connect cluster list   # → None found
confluent flink statement list   # → None found
confluent kafka cluster list     # → Sem "desafio-basic"
confluent environment list       # → Sem "desafio-final"
```

Para limpar o Neon:
```sql
DROP TABLE transactions, cards, accounts, merchants, customers CASCADE;
DROP PUBLICATION desafio_pub;
DROP ROLE cdc_user;
```

---

## Custo Total Declarado

Execute `confluent billing cost list` após rodar o pipeline e registre o valor total aqui.

Cluster Basic com free-tier para tasks/CFUs/schemas. Custo efetivo concentrado em network I/O durante o snapshot inicial do CDC.
