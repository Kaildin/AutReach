# ☁️ Opzioni Cloud per il tuo Scraping Pipeline

## 📊 Confronto Opzioni

| Soluzione | Costo/mese | Difficoltà | Scalabilità | Quando usarla |
|-----------|------------|------------|-------------|---------------|
| **Railway** | €5-20 | ⭐ Facile | Media | Inizio rapido, poche migliaia di aziende |
| **Render** | €7-25 | ⭐ Facile | Media | Simile a Railway, CI/CD integrato |
| **DigitalOcean Droplet** | €6-12 | ⭐⭐ Media | Alta | Controllo totale, costi prevedibili |
| **AWS Lambda + SQS** | €5-50 | ⭐⭐⭐ Difficile | Molto alta | Production, migliaia di job/giorno |
| **GCP Cloud Run** | €0-30 | ⭐⭐ Media | Alta | Pay-per-use, picchi variabili |

---

## 🚀 Opzione 1: Railway (CONSIGLIATA per iniziare)

### Vantaggi
✅ Deploy in 5 minuti con GitHub  
✅ Postgres incluso (per salvare progressi)  
✅ Cron jobs integrati  
✅ €5 di free tier al mese  

### Setup
```bash
# 1. Installa Railway CLI
npm i -g @railway/cli

# 2. Login
railway login

# 3. Crea progetto
railway init

# 4. Deploy
railway up
```

### File necessari: `railway.json`
```json
{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "python main_pipeline.py --output /data/companies.csv",
    "restartPolicyType": "ON_FAILURE"
  }
}
```

### Cron per admin pipeline
```yaml
# railway.yaml
services:
  main-scraper:
    schedule: "0 2 * * *"  # Ogni giorno alle 2am
    command: python main_pipeline.py
  
  admin-enricher:
    schedule: "0 4 * * *"  # Dopo main, alle 4am
    command: python admin_pipeline.py --input /data/companies.csv
```

**Costo stimato**: €10-15/mese per ~1000 aziende/giorno

---

## 🐳 Opzione 2: DigitalOcean Droplet (migliore prezzo/prestazioni)

### Vantaggi
✅ Costo fisso prevedibile (€6/mese)  
✅ Controllo completo  
✅ Puoi tenere Chrome headless  
✅ IP statico  

### Setup completo
```bash
# 1. Crea droplet Ubuntu 22.04 ($6/month)
# 2. SSH nel server
ssh root@your-droplet-ip

# 3. Installa dipendenze
apt update && apt upgrade -y
apt install -y python3.11 python3-pip chromium-browser git

# 4. Clone repo
git clone https://github.com/tuousername/scraping-pipeline.git
cd scraping-pipeline

# 5. Setup virtualenv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 6. Crea systemd service
nano /etc/systemd/system/scraper.service
```

### `scraper.service`
```ini
[Unit]
Description=Scraping Pipeline
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/scraping-pipeline
Environment="PATH=/root/scraping-pipeline/venv/bin"
ExecStart=/root/scraping-pipeline/venv/bin/python main_pipeline.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Cron per automazione
```bash
# Edita crontab
crontab -e

# Aggiungi jobs
0 2 * * * cd /root/scraping-pipeline && /root/scraping-pipeline/venv/bin/python main_pipeline.py >> /var/log/scraper.log 2>&1
0 4 * * * cd /root/scraping-pipeline && /root/scraping-pipeline/venv/bin/python admin_pipeline.py --input output/companies.csv >> /var/log/admin.log 2>&1
```

**Costo stimato**: €6/mese (fisso)

---

## ⚡ Opzione 3: AWS Lambda + SQS (per produzione seria)

### Vantaggi
✅ Scala automaticamente  
✅ Pay-per-use (€0.20 per 1M richieste)  
✅ Nessun server da gestire  
✅ Parallelismo estremo  

### Architettura
```
Google Maps Scraper (Lambda)
    ↓ (pubblica aziende)
SQS Queue
    ↓ (consuma jobs)
Admin Enricher (Lambda × 10 concurrent)
    ↓ (salva)
DynamoDB / S3
```

### `serverless.yml`
```yaml
service: scraping-pipeline

provider:
  name: aws
  runtime: python3.11
  region: eu-south-1
  environment:
    COMPANIES_TABLE: companies-table
    OPENAI_API_KEY: ${env:OPENAI_API_KEY}

functions:
  mainScraper:
    handler: handlers.main_scraper
    timeout: 900  # 15 min
    events:
      - schedule: cron(0 2 * * ? *)  # Ogni giorno 2am
    layers:
      - arn:aws:lambda:eu-south-1:xxxxx:layer:chrome:1
  
  adminEnricher:
    handler: handlers.admin_enricher
    timeout: 300  # 5 min
    events:
      - sqs:
          arn: !GetAtt CompaniesQueue.Arn
          batchSize: 10
    reservedConcurrency: 10  # Max 10 in parallelo

resources:
  Resources:
    CompaniesQueue:
      Type: AWS::SQS::Queue
      Properties:
        QueueName: companies-queue
        VisibilityTimeout: 360
```

### `handlers.py`
```python
import json
import boto3
from main_pipeline import run_main_pipeline
from admin_pipeline import run_admin_pipeline

sqs = boto3.client('sqs')
QUEUE_URL = os.environ['QUEUE_URL']

def main_scraper(event, context):
    """Lambda che scrapa Google Maps e mette in coda"""
    companies = run_main_pipeline(limit=1000)
    
    # Pubblica su SQS per processamento parallelo
    for company in companies:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(company)
        )
    
    return {'statusCode': 200, 'body': f'{len(companies)} companies queued'}

def admin_enricher(event, context):
    """Lambda che processa singola azienda da SQS"""
    for record in event['Records']:
        company = json.loads(record['body'])
        enriched = run_admin_pipeline([company])
        # Salva in DB
        save_to_dynamodb(enriched)
    
    return {'statusCode': 200}
```

**Costo stimato**: €5-20/mese per 10k aziende/mese

---

## 🎨 Opzione 4: Cloud Run (Google) - Bilanciamento ideale

### Vantaggi
✅ Container Docker (porta tutto)  
✅ Scala a zero (€0 quando non usi)  
✅ Trigger HTTP o Scheduler  
✅ Free tier generoso (2M richieste/mese)  

### `Dockerfile`
```dockerfile
FROM python:3.11-slim

# Installa Chrome
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Script che decide quale pipeline eseguire
CMD ["python", "cloud_runner.py"]
```

### `cloud_runner.py`
```python
import os
from flask import Flask, request
from main_pipeline import run_main_pipeline
from admin_pipeline import run_admin_pipeline

app = Flask(__name__)

@app.route('/scrape', methods=['POST'])
def scrape():
    data = request.json
    pipeline = data.get('pipeline', 'main')
    
    if pipeline == 'main':
        result = run_main_pipeline(limit=data.get('limit', 100))
    elif pipeline == 'admin':
        result = run_admin_pipeline(
            input_csv=data.get('input_csv'),
            output_csv=data.get('output_csv')
        )
    
    return {'status': 'ok', 'processed': len(result)}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
```

### Deploy
```bash
# 1. Build e push
gcloud builds submit --tag gcr.io/PROJECT_ID/scraper

# 2. Deploy
gcloud run deploy scraper \
  --image gcr.io/PROJECT_ID/scraper \
  --platform managed \
  --region europe-west1 \
  --memory 2Gi \
  --timeout 15m \
  --no-allow-unauthenticated

# 3. Schedule con Cloud Scheduler
gcloud scheduler jobs create http scraper-daily \
  --schedule="0 2 * * *" \
  --uri="https://scraper-xxx.run.app/scrape" \
  --http-method=POST \
  --message-body='{"pipeline":"main","limit":1000}'
```

**Costo stimato**: €0-15/mese (dipende dall'uso)

---

## 🗄️ Storage dei risultati

### Opzione A: CSV su S3/Cloud Storage (semplice)
```python
import boto3

s3 = boto3.client('s3')
s3.upload_file('output/companies.csv', 'my-bucket', 'data/companies.csv')
```

### Opzione B: Database (consigliato per produzione)
```python
# PostgreSQL su Railway/Render/Supabase
import psycopg2

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

cursor.execute("""
    INSERT INTO companies (nome, indirizzo, email, admin, status)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (nome) DO UPDATE SET
        email = EXCLUDED.email,
        admin = EXCLUDED.admin,
        updated_at = NOW()
""", (nome, indirizzo, email, admin, 'enriched'))
```

### Schema DB consigliato
```sql
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) UNIQUE NOT NULL,
    indirizzo TEXT,
    comune VARCHAR(100),
    provincia VARCHAR(50),
    sito_web VARCHAR(500),
    email VARCHAR(255),
    linkedin VARCHAR(500),
    admin_name VARCHAR(255),
    admin_role VARCHAR(100),
    status VARCHAR(50),  -- 'scraped', 'enriched', 'contacted'
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_status ON companies(status);
CREATE INDEX idx_comune ON companies(comune);
```

---

## 🎯 La mia raccomandazione per te

### Per iniziare (questo mese):
**DigitalOcean Droplet €6/mese**
- Deploy rapido
- Costi fissi
- Perfetto per validare il progetto

### Per scalare (tra 2-3 mesi):
**Railway + Postgres** o **Cloud Run + Cloud SQL**
- Automazione completa
- Monitoring integrato
- Pronto per clienti

### Setup immediato (5 minuti):
```bash
# 1. Crea Droplet DigitalOcean
# 2. SSH e installa
apt install python3-pip git -y
git clone YOUR_REPO
cd scraping-pipeline
pip3 install -r requirements.txt

# 3. Test
python3 main_pipeline.py --limit 10

# 4. Cron per automazione
echo "0 2 * * * cd /root/scraping-pipeline && python3 main_pipeline.py" | crontab -
```

Vuoi che ti prepari gli script completi per una delle opzioni? Dimmi quale preferisci! 🚀
