# mlvizz-grafana-local

**Author:** Joel Antony — *Data Engineer Intern @ SimplyAI*  
**Description:**  
This repository automates the full setup of a **Grafana + PostgreSQL** stack for insurance analytics visualization (MLVizz project).  
It provisions containers, restores the insurance database, and automatically creates a Grafana dashboard — no manual UI steps required.

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository
cd mlvizz-grafana-local

2️⃣ Start Docker Containers: 
docker compose up -d

This starts:
pg_insurance (PostgreSQL)
grafana_insurance (Grafana)

3️⃣ Restore the Insurance Database
docker exec -e PGPASSWORD=postgres -it pg_insurance \
  pg_restore -U postgres -h localhost -p 5432 -d insurance \
  --clean --if-exists --no-owner --no-privileges --verbose \
  /backups/insurance.dump
✅ This creates and populates:
agents, claims, insured_items, policies, policyholders

4️⃣ Install Python Dependencies
python3 -m pip install requests

5️⃣ Run the Provisioning Script
python3 provision_grafana_mlvizz.py


This script:

Connects to Grafana (http://localhost:3000)
Authenticates using admin / admin
Ensures a PostgreSQL data source is configured
Creates a MLVizz folder
Deploys the dashboard MLVizz Insurance Overview
