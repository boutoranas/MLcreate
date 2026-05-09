Group Members:

- Anas Boutor, SID: 21010333, aboutor@connect.ust.hk
- Almat Zhezbayev, SID: 20901779, azhezbayev@connect.ust.hk

# MLcreate

A cloud-native ML pipeline: upload a CSV, train a model (XGBoost/linear, optionally distributed via Spark), and run predictions — all through a Next.js web UI backed by AWS SQS, S3, and PostgreSQL running on EC2.

---

## Architecture overview

```
Browser (Next.js) → SQS → EC2 Docker consumers
                              ├── ingest_consumer   (validates CSV, saves to S3)
                              ├── preprocess_consumer (normalises, outputs Parquet)
                              ├── train_consumer    (XGBoost / Spark MLlib → S3)
                              └── predict_consumer  (loads model, outputs CSV → S3)
                    ↕
               PostgreSQL (jobs / models tables)
               S3 (uploads / processed / models / predictions_output)
```

---

## Prerequisites

| Tool                              | Version    |
| --------------------------------- | ---------- |
| Node.js                           | 18+        |
| Docker + Docker Compose           | any recent |
| AWS Academy session (or IAM user) | —          |

---

## 1. AWS setup (do this every session if using AWS Academy)

AWS Academy credentials expire per session. Repeat steps 1.1–1.3 each time you start a new session.

### 1.1 Create S3 bucket

1. Open the S3 console → **Create bucket**
2. Give it a unique name, e.g. `cloudml-yourname-2026`
3. Region: `us-east-1` (or adjust everywhere below)
4. Leave all defaults → **Create bucket**

### 1.2 Create SQS queues

Create these five **Standard** queues (default settings are fine):

| Queue name                     |
| ------------------------------ |
| `cloudml-csv-upload-requested` |
| `cloudml-dataset-uploaded`     |
| `cloudml-preprocessing-done`   |
| `cloudml-predict-requested`    |
| `cloudml-training-complete`    |

Steps per queue: SQS console → **Create queue** → Standard → set name → **Create queue**.

### 1.3 Get credentials

AWS Academy → **AWS Details** → **AWS CLI** → copy the three lines that look like:

```
AWS_ACCESS_KEY_ID=ASIA...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
```

---

## 2. Local environment file

Copy the example and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# PostgreSQL (used by Next.js and train_consumer)
DATABASE_URL=postgresql+psycopg2://cloudml:cloudml@localhost:5432/cloudml

# Directories (used inside Docker containers — leave as-is)
MODELS_DIR=models
PROCESSED_DIR=processed

# AWS — paste from step 1.3
AWS_ACCESS_KEY_ID=ASIA...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_DEFAULT_REGION=us-east-1

# S3 bucket name from step 1.1
S3_BUCKET=cloudml-yourname-2026

# SQS queue names (match what you created in step 1.2)
SQS_QUEUE_CSV_UPLOAD_REQUESTED=cloudml-csv-upload-requested
SQS_QUEUE_DATASET_UPLOADED=cloudml-dataset-uploaded
SQS_QUEUE_PREPROCESSING_DONE=cloudml-preprocessing-done
SQS_QUEUE_PREDICT_REQUESTED=cloudml-predict-requested
SQS_QUEUE_TRAINING_COMPLETE=cloudml-training-complete
```

---

## 3. Clerk authentication

MLcreate uses [Clerk](https://clerk.com) for auth. Create a free account if you don't have one.

1. Create a new application in the Clerk dashboard
2. Copy the keys from **API Keys**:

```env
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_test_...
CLERK_SECRET_KEY=sk_test_...
```

Add these to your `.env` file (or to Vercel environment variables if deploying there).

---

## 4. Start the database

```bash
docker compose up -d postgres
```

The schema is applied automatically from `db/schema.sql` on first start.
Verify it's up: `docker compose ps` — postgres should show `healthy`.

---

## 5. Next.js — local dev server

```bash
npm install
npm run dev
```

App runs at `http://localhost:3000`.

---

## 5a. Deploy the frontend to Vercel (optional)

The Next.js app can be deployed to Vercel while the consumers keep running on EC2. Vercel calls the same SQS queues and S3 bucket; EC2 does all the heavy work.

### One-time setup

1. Push the repo to GitHub (already done if you cloned from there)
2. Go to [vercel.com](https://vercel.com) → **Add New Project** → import the GitHub repo
3. Framework preset: **Next.js** (auto-detected)
4. Click **Deploy** — the first build will fail because env vars are missing; that's fine

### Add environment variables in Vercel

In the project → **Settings → Environment Variables**, add every variable from the table in section 9:

| Variable                            | Value                                                         |
| ----------------------------------- | ------------------------------------------------------------- |
| `DATABASE_URL`                      | PostgreSQL URL pointing at your EC2 instance (see note below) |
| `AWS_ACCESS_KEY_ID`                 | From AWS Academy                                              |
| `AWS_SECRET_ACCESS_KEY`             | From AWS Academy                                              |
| `AWS_SESSION_TOKEN`                 | From AWS Academy                                              |
| `AWS_DEFAULT_REGION`                | `us-east-1`                                                   |
| `S3_BUCKET`                         | Your bucket name                                              |
| `SQS_QUEUE_CSV_UPLOAD_REQUESTED`    | `cloudml-csv-upload-requested`                                |
| `SQS_QUEUE_DATASET_UPLOADED`        | `cloudml-dataset-uploaded`                                    |
| `SQS_QUEUE_PREPROCESSING_DONE`      | `cloudml-preprocessing-done`                                  |
| `SQS_QUEUE_PREDICT_REQUESTED`       | `cloudml-predict-requested`                                   |
| `SQS_QUEUE_TRAINING_COMPLETE`       | `cloudml-training-complete`                                   |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | From Clerk dashboard                                          |
| `CLERK_SECRET_KEY`                  | From Clerk dashboard                                          |

After saving, go to **Deployments → Redeploy** (or push a new commit).

### Making PostgreSQL reachable from Vercel

Vercel runs on Vercel's servers, so `localhost:5432` won't work. Two options:

**Option A — expose EC2 postgres (simplest for Academy)**

1. EC2 Security Group → add inbound rule: **PostgreSQL (5432)** from `0.0.0.0/0` (or restrict to Vercel IP ranges)
2. Set `DATABASE_URL` in Vercel to:
   ```
   postgresql+psycopg2://cloudml:cloudml@<EC2_PUBLIC_IP>:5432/cloudml
   ```

**Option B — use a managed database (more robust)**
Neon and Supabase both offer free-tier Postgres. Create a database, copy the connection string, and use that as `DATABASE_URL` in both Vercel and your EC2 `.env`.

### Updating AWS credentials on Vercel (each Academy session)

AWS Academy tokens expire. Each new session:

1. Copy the three new credential lines from AWS Details
2. In Vercel → **Settings → Environment Variables** → update `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
3. Redeploy (or push any commit to trigger a new build)

---

## 6. Start the SQS consumers on EC2

The four consumers (`ingest`, `preprocess`, `train`, `predict`) run as Docker containers and must be on a machine with outbound AWS access. In production this is an EC2 instance.

### 6.1 Launch EC2 instance (first time only)

1. EC2 console → **Launch instance**
2. AMI: **Amazon Linux 2023** (or Ubuntu 22.04)
3. Instance type: `t3.medium` minimum (`t3.large` recommended for Spark training)
4. Security group: allow inbound SSH (port 22) from your IP
5. Add an IAM instance profile with `AmazonSQSFullAccess` and `AmazonS3FullAccess` — or pass credentials via `.env`
6. Launch and note the public IP

### 6.2 Install Docker on the instance (first time only)

```bash
ssh -i your-key.pem ec2-user@<EC2_IP>

sudo yum update -y
sudo yum install -y docker git
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
newgrp docker

# Docker Compose v2
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 6.3 Clone the repo (first time only)

```bash
git clone https://github.com/boutoranas/MLcreate.git
cd MLcreate
```

### 6.4 Create `.env` on EC2

Same content as step 2. Copy from your local machine:

```bash
scp -i your-key.pem .env ec2-user@<EC2_IP>:~/MLcreate/.env
```

Or create it directly on the instance with a text editor (`nano .env`).

### 6.5 Add runtime dirs to `.gitignore` (first time only)

Docker containers run as root and write files that would block future `git pull`:

```bash
cat >> .gitignore << 'EOF'
messages/
processed/
models/
data/
EOF
```

### 6.6 Build and start consumers

```bash
docker compose up -d --build ingest_consumer preprocess_consumer train_consumer predict_consumer postgres
```

Check they are running:

```bash
docker compose ps
docker compose logs -f train_consumer   # ctrl-c to stop following
```

---

## 7. Updating EC2 after a code change

```bash
ssh -i your-key.pem ec2-user@<EC2_IP>
cd MLcreate

# Remove root-owned runtime files that would block git pull
sudo rm -rf processed/ models/ data/
git clean -f messages/

git pull origin main

# Rebuild the affected consumer (--no-cache ensures new code is picked up)
docker compose build --no-cache train_consumer
docker compose up -d train_consumer
```

> Always use `docker compose build --no-cache` when Python code or dependencies changed. The `--build` flag on `up` reuses cached layers and may miss changes.

---

## 8. End-to-end flow

1. Open `http://localhost:3000`, sign in via Clerk
2. **Create model**: upload a CSV with a `label` column, choose classification or regression
3. The status page polls `/api/job-status/<id>` and transitions: `queued → ingested → training → completed`
4. Once completed, open the model page and upload a CSV for prediction
5. Status polls every 5 s; a **Download predictions CSV** button appears when the output lands in S3

---

## 9. Environment variables reference

| Variable                            | Where used                            | Description                                      |
| ----------------------------------- | ------------------------------------- | ------------------------------------------------ |
| `DATABASE_URL`                      | Next.js, train_consumer               | SQLAlchemy-style PostgreSQL URL                  |
| `AWS_ACCESS_KEY_ID`                 | All                                   | AWS credential                                   |
| `AWS_SECRET_ACCESS_KEY`             | All                                   | AWS credential                                   |
| `AWS_SESSION_TOKEN`                 | All                                   | Academy session token (rotate each session)      |
| `AWS_DEFAULT_REGION`                | All                                   | Default: `us-east-1`                             |
| `S3_BUCKET`                         | All                                   | Bucket for uploads, parquet, models, predictions |
| `SQS_QUEUE_CSV_UPLOAD_REQUESTED`    | Next.js upload API, ingest_consumer   | First queue in the pipeline                      |
| `SQS_QUEUE_DATASET_UPLOADED`        | ingest_consumer, preprocess_consumer  | After ingest                                     |
| `SQS_QUEUE_PREPROCESSING_DONE`      | preprocess_consumer, train_consumer   | After preprocessing                              |
| `SQS_QUEUE_PREDICT_REQUESTED`       | Next.js predict API, predict_consumer | Trigger prediction                               |
| `SQS_QUEUE_TRAINING_COMPLETE`       | train_consumer                        | Published after training                         |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | Next.js client                        | Clerk publishable key                            |
| `CLERK_SECRET_KEY`                  | Next.js server                        | Clerk secret key                                 |
| `MODELS_DIR`                        | train_consumer, predict_consumer      | Local path inside container                      |
| `PROCESSED_DIR`                     | preprocess_consumer, train_consumer   | Local path inside container                      |

---

## 10. S3 bucket layout

```
<bucket>/
  uploads/<job_id>/<filename>.csv          ← raw upload
  processed/<job_id>.parquet               ← after preprocessing
  models/<job_id>.pkl                      ← sklearn/XGBoost model
  models/<job_id>.pkl.spark/               ← Spark MLlib model directory
  models/<job_id>.pkl.meta                 ← training metadata JSON
  predictions_output/<predict_id>.csv      ← prediction results
```

---

## 11. Database schema

```sql
jobs    (job_id, model_name, task_type, uploader, status, user_id, created_at)
models  (job_id, model_path, user_id, created_at)
metrics (job_id, metric_name, metric_value, recorded_at)
```

Schema is applied automatically when postgres starts via `db/schema.sql`.

---

## Troubleshooting

**Job stuck on "training"**
Check `docker compose logs train_consumer`. If you see DB errors, verify `DATABASE_URL` in `.env` on EC2 points to the reachable postgres host.

**`git pull` blocked by untracked files**

```bash
sudo rm -rf processed/ models/ data/
git clean -f messages/
git pull origin main
```

**Consumers not picking up messages**
AWS Academy credentials expire. Re-copy fresh credentials into `.env` on EC2 and restart consumers:

```bash
docker compose up -d ingest_consumer preprocess_consumer train_consumer predict_consumer
```

**Prediction stuck on "Waiting for results…"**
Check `docker compose logs predict_consumer`. The output CSV must reach `predictions_output/<predict_id>.csv` in S3.
