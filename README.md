# EMR Serverless ETL Pipeline — Medallion Architecture

## 🇺🇸 English

### Overview

End-to-end data engineering project using AWS EMR Serverless to process shipment data through a Medallion Architecture (Bronze → Silver). The pipeline reads raw CSV data from S3, applies transformations and data quality validations using PySpark, and writes the results as partitioned Parquet files back to S3.

### Architecture

```
S3 (CSV) → EMR Serverless (Spark Job - Bronze) → S3 (Parquet)
S3 (CSV) → EMR Serverless (Spark Job - Silver) → S3 (Parquet)
```

### What each layer does

- **Bronze**: Reads raw CSV, filters invalid time records, saves as Parquet partitioned by Year/Month
- **Silver**: Reads raw CSV, applies all Bronze filters plus: timestamp conversion, travel time calculation, route ID creation, data quality validations (distance, travel time, outliers, carrier, source/destination codes, planned time consistency, delay consistency), and delay categorization (OnTime, Minor, Moderate, Severe)

### Tech Stack

- AWS EMR Serverless (Spark)
- AWS S3 (storage)
- AWS IAM (least privilege access)
- PySpark
- AWS CLI

---

## 🇧🇷 Português

### Visão Geral

Projeto de engenharia de dados end-to-end usando AWS EMR Serverless para processar dados de envios através de uma Arquitetura Medallion (Bronze → Silver). O pipeline lê dados CSV brutos do S3, aplica transformações e validações de qualidade usando PySpark, e grava os resultados como arquivos Parquet particionados de volta no S3.

### Arquitetura

```
S3 (CSV) → EMR Serverless (Spark Job - Bronze) → S3 (Parquet)
S3 (CSV) → EMR Serverless (Spark Job - Silver) → S3 (Parquet)
```

### O que cada camada faz

- **Bronze**: Lê o CSV bruto, filtra registros com horários inválidos, salva como Parquet particionado por Year/Month
- **Silver**: Lê o CSV bruto, aplica todos os filtros da Bronze mais: conversão de timestamps, cálculo de tempo real de viagem, criação de Route ID, validações de qualidade (distância, tempo de viagem, outliers, carrier, códigos de origem/destino, consistência de tempo planejado, consistência de delay) e categorização de atraso (OnTime, Minor, Moderate, Severe)

### Stack

- AWS EMR Serverless (Spark)
- AWS S3 (armazenamento)
- AWS IAM (acesso com privilégio mínimo)
- PySpark
- AWS CLI

### Passo a passo com comandos

#### 1. Configurar acesso SSO via AWS CLI

```bash
aws configure sso
```

Será solicitado:
- SSO session name: `[NOME_DA_SESSAO]`
- SSO start URL: `https://[SUA_ORG].awsapps.com/start`
- SSO region: `us-east-1`
- SSO registration scopes: aperte Enter (padrão)
- Região padrão: `us-east-1`
- Nome do profile: `[NOME_DO_PROFILE]`

Verificar a conexão:

```bash
aws sts get-caller-identity --profile [NOME_DO_PROFILE]
```

#### 2. Criar bucket S3

```bash
aws s3api create-bucket --bucket [NOME_DO_BUCKET] --region us-east-1 --profile [NOME_DO_PROFILE]
```

#### 3. Subir CSV e scripts PySpark para o S3

```bash
aws s3 cp data/fedex.csv s3://[NOME_DO_BUCKET]/data/fedex.csv --profile [NOME_DO_PROFILE]

aws s3 cp scripts/camada_bronze.py s3://[NOME_DO_BUCKET]/scripts/camada_bronze.py --profile [NOME_DO_PROFILE]

aws s3 cp scripts/camada_silver.py s3://[NOME_DO_BUCKET]/scripts/camada_silver.py --profile [NOME_DO_PROFILE]
```

#### 4. Criar IAM Role com trust policy para o EMR Serverless

A trust policy (`policies/trust-policy.json`) permite que o serviço EMR Serverless assuma essa role:

```bash
aws iam create-role \
  --role-name EMRServerlessJobRole \
  --assume-role-policy-document file://policies/trust-policy.json \
  --profile [NOME_DO_PROFILE]
```

#### 5. Anexar policy de privilégio mínimo

A policy de execução (`policies/emr-policy.json`) concede apenas as permissões necessárias:
- **Leitura**: `s3:GetObject` e `s3:ListBucket` restrito aos paths `data/` e `scripts/`
- **Escrita**: `s3:PutObject` e `s3:DeleteObject` restrito ao path `output/`
- **Logs**: Criação de logs no CloudWatch restrita à conta

```bash
aws iam put-role-policy \
  --role-name EMRServerlessJobRole \
  --policy-name EMRServerlessJobPolicy \
  --policy-document file://policies/emr-policy.json \
  --profile [NOME_DO_PROFILE]
```

#### 6. Criar aplicação EMR Serverless

```bash
aws emr-serverless create-application \
  --release-label emr-7.1.0 \
  --type SPARK \
  --name "medallion-pipeline" \
  --profile [NOME_DO_PROFILE]
```

Anote o `applicationId` da resposta — será necessário para submeter os jobs.

#### 7. Submeter job Bronze

Edite o `job-driver.json` e defina o `entryPoint` para o script Bronze:

```json
{
    "sparkSubmit": {
        "entryPoint": "s3://[NOME_DO_BUCKET]/scripts/camada_bronze.py",
        "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
    }
}
```

Submeter o job:

```bash
aws emr-serverless start-job-run \
  --application-id [APPLICATION_ID] \
  --execution-role-arn arn:aws:iam::[ACCOUNT_ID]:role/EMRServerlessJobRole \
  --job-driver file://job-driver.json \
  --profile [NOME_DO_PROFILE]
```

Acompanhar o status:

```bash
aws emr-serverless get-job-run \
  --application-id [APPLICATION_ID] \
  --job-run-id [JOB_RUN_ID] \
  --profile [NOME_DO_PROFILE]
```

Estados: `SUBMITTED` → `PENDING` → `SCHEDULED` → `RUNNING` → `SUCCESS`

#### 8. Submeter job Silver

Atualize o `entryPoint` no `job-driver.json` para `camada_silver.py` e submeta novamente:

```bash
aws emr-serverless start-job-run \
  --application-id [APPLICATION_ID] \
  --execution-role-arn arn:aws:iam::[ACCOUNT_ID]:role/EMRServerlessJobRole \
  --job-driver file://job-driver.json \
  --profile [NOME_DO_PROFILE]
```

#### 9. Verificar output Parquet no S3

```bash
aws s3 ls s3://[NOME_DO_BUCKET]/output/fedex_bronze/ --recursive --profile [NOME_DO_PROFILE]

aws s3 ls s3://[NOME_DO_BUCKET]/output/fedex_silver/ --recursive --profile [NOME_DO_PROFILE]
```

Estrutura esperada:

```
output/fedex_bronze/
├── Year=2008/
│   ├── Month=1/
│   │   ├── part-00000-xxxx.parquet
│   │   └── ...
│   ├── Month=2/
│   │   └── ...
│   └── ...
└── _SUCCESS
```

### Policy IAM — Privilégio Mínimo na prática

A role de execução só permite operações de leitura/escrita com escopo restrito. Para validar a eficácia da policy, foi feita uma submissão adicional do job tentando replicar a escrita no mesmo path de output — a policy bloqueou a operação de sobrescrita não autorizada, comprovando que a abordagem de privilégio mínimo funciona na prática.

### Estrutura do Projeto

```
├── scripts/
│   ├── utils.py            # Funções compartilhadas entre os jobs
│   ├── camada_bronze.py    # Job da camada Bronze
│   └── camada_silver.py    # Job da camada Silver
├── policies/
│   ├── trust-policy.json   # Trust policy para o EMR Serverless
│   └── emr-policy.json     # Permissões da role de execução
├── job-driver.json          # Configuração do job Spark
└── README.md
```
