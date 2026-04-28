# EMR Serverless ETL Pipeline — Medallion Architecture

##  Português

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

---

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

### Step-by-step with commands

#### 1. Configure AWS SSO CLI access

```bash
aws configure sso
```

You will be prompted for:
- SSO session name: `[SESSION_NAME]`
- SSO start URL: `https://[YOUR_ORG].awsapps.com/start`
- SSO region: `us-east-1`
- SSO registration scopes: press Enter (default)
- Default region: `us-east-1`
- Profile name: `[PROFILE_NAME]`

Verify the connection:

```bash
aws sts get-caller-identity --profile [PROFILE_NAME]
```

#### 2. Create S3 bucket

```bash
aws s3api create-bucket --bucket [BUCKET_NAME] --region us-east-1 --profile [PROFILE_NAME]
```

#### 3. Upload CSV data and PySpark scripts to S3

```bash
aws s3 cp data/fedex.csv s3://[BUCKET_NAME]/data/fedex.csv --profile [PROFILE_NAME]

aws s3 cp scripts/camada_bronze.py s3://[BUCKET_NAME]/scripts/camada_bronze.py --profile [PROFILE_NAME]

aws s3 cp scripts/camada_silver.py s3://[BUCKET_NAME]/scripts/camada_silver.py --profile [PROFILE_NAME]
```

#### 4. Create IAM Role with trust policy for EMR Serverless

The trust policy (`policies/trust-policy.json`) allows the EMR Serverless service to assume this role:

```bash
aws iam create-role \
  --role-name EMRServerlessJobRole \
  --assume-role-policy-document file://policies/trust-policy.json \
  --profile [PROFILE_NAME]
```

#### 5. Attach least privilege policy

The execution policy (`policies/emr-policy.json`) grants only the necessary permissions:
- **Read**: `s3:GetObject` and `s3:ListBucket` scoped to `data/` and `scripts/` paths
- **Write**: `s3:PutObject` and `s3:DeleteObject` scoped to `output/` path only
- **Logs**: CloudWatch log creation scoped to the account

```bash
aws iam put-role-policy \
  --role-name EMRServerlessJobRole \
  --policy-name EMRServerlessJobPolicy \
  --policy-document file://policies/emr-policy.json \
  --profile [PROFILE_NAME]
```

#### 6. Create EMR Serverless application

```bash
aws emr-serverless create-application \
  --release-label emr-7.1.0 \
  --type SPARK \
  --name "medallion-pipeline" \
  --profile [PROFILE_NAME]
```

Save the `applicationId` from the response — you will need it to submit jobs.

#### 7. Submit Bronze job

Edit `job-driver.json` and set the `entryPoint` to your Bronze script path:

```json
{
    "sparkSubmit": {
        "entryPoint": "s3://[BUCKET_NAME]/scripts/camada_bronze.py",
        "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
    }
}
```

Submit the job:

```bash
aws emr-serverless start-job-run \
  --application-id [APPLICATION_ID] \
  --execution-role-arn arn:aws:iam::[ACCOUNT_ID]:role/EMRServerlessJobRole \
  --job-driver file://job-driver.json \
  --profile [PROFILE_NAME]
```

Monitor the job status:

```bash
aws emr-serverless get-job-run \
  --application-id [APPLICATION_ID] \
  --job-run-id [JOB_RUN_ID] \
  --profile [PROFILE_NAME]
```

States: `SUBMITTED` → `PENDING` → `SCHEDULED` → `RUNNING` → `SUCCESS`

#### 8. Submit Silver job

Update `job-driver.json` entryPoint to `camada_silver.py` and submit again:

```bash
aws emr-serverless start-job-run \
  --application-id [APPLICATION_ID] \
  --execution-role-arn arn:aws:iam::[ACCOUNT_ID]:role/EMRServerlessJobRole \
  --job-driver file://job-driver.json \
  --profile [PROFILE_NAME]
```

#### 9. Verify Parquet output in S3

```bash
aws s3 ls s3://[BUCKET_NAME]/output/fedex_bronze/ --recursive --profile [PROFILE_NAME]

aws s3 ls s3://[BUCKET_NAME]/output/fedex_silver/ --recursive --profile [PROFILE_NAME]
```

Expected output structure:

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

### IAM Policy — Least Privilege in practice

The execution role only allows scoped read/write operations. During testing, a duplicate job was accidentally submitted — the policy blocked the unauthorized overwrite attempt, proving the least privilege approach works in practice.

### Project Structure

```
├── scripts/
│   ├── camada_bronze.py    # Bronze layer job
│   └── camada_silver.py    # Silver layer job
├── policies/
│   ├── trust-policy.json   # Trust policy for EMR Serverless
│   └── emr-policy.json     # Execution role permissions
├── job-driver.json          # Spark job configuration
└── README.md
```
