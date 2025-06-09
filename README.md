# Databricks + Power BI Healthcare Project (In Progress)

This project demonstrates an end-to-end healthcare data analytics workflow using **synthetic EHR data from Synthea**. It models a real-world scenario—shifting from SQL Server–style logic to a modern Databricks + Power BI pipeline—to build scalable, payer-facing reporting and insights.

---

## 🔍 Project Purpose

- Simulate a modern healthcare data stack using **Databricks + Power BI**
- Apply **PySpark and SQL** in Databricks notebooks for realistic EHR data workflows
- Build a **Power BI dashboard** backed by a star schema using Power Query and DAX
- Showcase **full-stack skills** in ETL, dimensional modeling, and interactive visualization

---

## 🧰 Tech Stack

### **Databricks**
- PySpark for cleaning, transforming, and aggregating synthetic EHR data
- SQL for exploratory queries and metric validation
- Notebook-based workflows simulating modular ETL pipelines
- Delta Lake layers (Bronze → Silver → Gold)

### **Power BI**
- Power Query for ingestion and semantic shaping
- DAX for calculated fields and healthcare KPIs
- Interactive dashboards (e.g., encounter trends, top conditions, demographic filters)

### **Cloud Storage**
- Simulated via **DBFS** (Databricks File System)

---

## 🏥 Data Source

**[Synthea](https://github.com/synthetichealth/synthea)** – Synthetic Patient EHR Generator  
- Patient-level files: `patients.csv`, `encounters.csv`, `conditions.csv`, etc.  
- Modeled star schema includes:
  - `fact_encounters`, `fact_conditions`
  - `dim_patient`, `dim_provider`, `dim_diagnosis`, `dim_date`

---

## ⚙️ Workflow Automation (In Progress)

This project simulates a **Databricks Job Workflow** for cloud-native ETL orchestration and a medallion architecture:

- **Bronze Layer**: raw Synthea CSV ingestion
- **Silver Layer**: cleaned, normalized star schema tables
- **Gold Layer**: aggregated metrics for reporting

Future plans:
- YAML-style job definition
- Modular notebook tasks
- Integration with dbx or REST API-based deployment

---

## 📊 Power BI Dashboard (Coming Soon)

An embedded Power BI dashboard will visualize:
- Encounter volume trends by provider and diagnosis
- Patient demographics
- Most common conditions and procedures
- Flexible filtering via dimension tables

---

## 💡 Real-World Relevance

This project reflects patterns commonly found in modern healthcare data teams:
- Replacing siloed SQL workflows with **collaborative notebooks and jobs**
- Creating **clean, reusable models** to power BI tools
- Bridging engineering and reporting layers in an **Agile data team**
