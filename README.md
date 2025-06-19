# Databricks + Power BI Healthcare Project (In Progress)

This project demonstrates an end-to-end healthcare data analytics workflow using synthetic EHR data from Synthea. It models a typical data modernization initiative by showing how to transition from embedded logic in SQL Server and Power BI into a scalable, modular architecture using **Databricks**.

Leveraging the **Medallion architecture** and **SQL views**, this project illustrates how analysts and data teams can centralize logic, optimize performance, and lay the foundation for governed, reusable insights across tools like **Power BI**.
---

## Project Purpose

- Clean and standardize Synthea-generated healthcare data using PySpark in Databricks notebooks (Bronze and Silver layers)
- Design optimized Gold-layer tables to support a star schema for analytics and dashboarding
- Replace embedded Power BI logic with reusable and centralized SQL views inside Databricks
- Surface insights on high-cost patients, condition prevalence, and readmission risks
- Showcase full-stack analyst skills across ETL, dimensional modeling, and interactive dashboarding
- Simulate a modern stack by connecting Databricks SQL views to Power BI using Power Query and DAX

---
## Tech Stack

- **Databricks Community Edition** – Scalable cloud environment for PySpark and Delta Lake development  
- **PySpark** – Data transformation and cleaning at scale  
- **Delta Lake** – Versioned, ACID-compliant storage layer for Bronze/Silver/Gold architecture  
- **SQL** – View creation within Databricks SQL notebooks for Power BI integration  
- **Power BI** – Dashboard development with DAX and Power Query (simulated connection)  
- **Synthea** – Synthetic EHR data source modeled after real-world healthcare records  
- **GitHub** – Version control and public portfolio sharing

---
## Medallion Architecture Overview

This project follows the Medallion architecture pattern to structure data pipelines for clarity, reusability, and performance. Each layer serves a specific role:

| Layer   | Description                                                                 | Format         | Source Location           |
|---------|-----------------------------------------------------------------------------|----------------|---------------------------|
| Bronze  | Raw CSVs ingested into Delta Lake format with metadata                     | Raw Delta      | `/FileStore/bronze/*`     |
| Silver  | Cleaned, typed, and normalized data with string trimming and type casting  | Refined Delta  | `/FileStore/silver/*`     |
| Gold    | Star schema: fact and dimension tables modeled for analytical use cases    | Modeled Delta  | `/FileStore/gold/*`       |

> _Add pipeline diagram here to illustrate table flow_

---
## Gold Layer Tables

The Gold layer models a star schema optimized for analytics and dashboarding. It consists of fact and dimension tables designed for slicing, aggregation, and exploration.

| Table Name             | Type        | Description                                                                 |
|------------------------|-------------|-----------------------------------------------------------------------------|
| `fact_encounters`      | Fact         | Encounter-level fact table with timestamps, costs, duration, and org info  |
| `fact_conditions`      | Fact         | Patient condition records with coded diagnosis and derived condition type  |
| `fact_patient_summary` | Fact         | Patient-level summary of encounters, conditions, and derived attributes     |
| `dim_patients`         | Dimension    | Patient demographics including gender, age group, race, ethnicity, income  |
| `dim_providers`        | Dimension    | Provider metadata with specialty and location                               |
| `dim_organizations`    | Dimension    | Organization metadata with location and geographic identifiers              |

<img src = "https://raw.githubusercontent.com/sarahrosegallagher/dbx-powerbi-health-project/refs/heads/main/imgs/gold_schema.png" width="900">


---
## SQL Views for Power BI Integration

These SQL views encapsulate complex business logic to replace embedded transformations in Power BI. They promote reusability, version control, and clearer collaboration between analysts and engineers.

| View Name                   | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| `encounter_summary_view`   | Core view for Power BI dashboards with encounter details and demographics   |
| `condition_summary_view`   | Aggregates condition prevalence across patient age groups and genders       |
| `diagnosis_cost_view`      | Computes average claim costs per diagnosis code or condition category       |
| `high_cost_patients_view`  | Identifies patients with total claim costs exceeding defined thresholds     |
| `readmission_candidate_view` | Flags possible readmission events based on encounter timing per patient    |

---
## Notes & Optimization Considerations

This project includes several optimizations to reflect production-grade data workflows and demonstrate analytical engineering best practices in Databricks:

### Optimization Techniques Applied

- **Medallion Architecture**: Adopted the Bronze → Silver → Gold layered approach to separate raw ingestion, cleansing, and analytics-ready data.
- **Broadcast Joins**: Used for dimension tables (e.g. organizations) to minimize shuffle during joins with large fact tables.
- **Repartitioning for Aggregations**: Applied `.repartition("patient")` before groupBy operations (e.g. patient summary aggregations) to reduce skew and improve parallelism.
- **Age Calculation Logic**: Accounted for deceased patients by using `deathdate` where available to calculate age.
- **Delta Lake Writes with Schema Control**:
  - Used `mergeSchema` and `autoMerge` to handle schema evolution safely.
  - Registered Gold tables to Hive metastore to support SQL querying and Power BI integration.

### SQL View Strategy

- **Reusable Views**: Created centralized views to house logic that would typically reside in Power BI, reducing duplication and enabling shared metrics across dashboards.
- **Semantic Layer Alignment**: Views align with common reporting needs like readmission tracking, cost benchmarking, and demographic stratification.

### Cluster and Performance Awareness

- **View Creation Instead of Materialization**: Views defer computation until queried, which is suitable for exploratory Power BI work.
- **Selective Column Use and Filtering**: Ensured only necessary columns were selected in Gold and view logic to reduce I/O and memory usage.
- **No Partitioning Yet on Gold Tables**: For simplicity and local execution in Community Edition. In production, Gold tables would benefit from partitioning (e.g. by year or payer) for improved performance at scale.

### Cloud Platform Considerations

- In an enterprise environment, data would typically reside in a **cloud data lake** (e.g. Azure Data Lake or AWS S3).
- Bronze data would be **read/written directly from cloud object storage** using Delta format.
- Power BI could connect to **Databricks SQL warehouse** over JDBC/ODBC, supported by **Unity Catalog** and **role-based access** to secure datasets.
- Community Edition simulates this by using local `/FileStore` paths, but the structure is designed to scale to cloud platforms with minimal changes.

### Future Considerations

- Partition large fact tables by temporal or payer fields to support performance at scale.
- Materialize frequently used SQL views into pre-aggregated Gold summary tables if latency becomes an issue.
- Use job workflows or pipelines to schedule data refreshes and orchestrate end-to-end processes.

> _These choices reflect a focus on maintainability, performance, and scalable design suitable for enterprise-level analytics._


---
## Power BI Dashboard (In Progress)

The Power BI dashboard will visualize key insights derived from the Gold tables and SQL views, enabling stakeholders to explore patterns in healthcare utilization, cost, and demographics. This section outlines proposed visuals aligned with common use cases in payer, provider, and health analytics environments.

### Proposed Visualizations

| Visualization                            | Description                                                                 |
|------------------------------------------|-----------------------------------------------------------------------------|
| Condition Prevalence by Age Group        | Shows top conditions across different patient age buckets                   |
| Multi-Condition Burden                   | Highlights how many patients have multiple chronic or acute conditions      |
| Avg Claim Cost per Diagnosis Group       | Visualizes cost differences between types of conditions                     |
| Utilization by Encounter Type            | Compares visit volume across inpatient, outpatient, and other settings      |
| Cost Variation by Geography              | Maps total and average claim cost by ZIP or state                          |
| Readmission Patterns                     | Flags patients with closely spaced encounters to identify repeat visits     |
| Demographics of High-Cost Patients       | Breaks down income, race, and gender for patients in top cost percentiles   |
| Outlier Encounter Costs                  | Displays extremely high-cost visits by diagnosis, provider, or setting      |

> _Visuals will be connected to SQL views and optimized Gold tables using Power Query and DAX. Screenshots and report links will be added upon completion._

