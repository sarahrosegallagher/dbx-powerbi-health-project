-- Databricks notebook source
-- 📊 condition_summary_view
-- Purpose: Aggregate condition counts and prevalence by age group and demographic info
CREATE OR REPLACE VIEW condition_summary_view AS
SELECT 
    c.code AS condition_code,
    c.description AS condition_description,
    p.age_group,
    p.gender,
    p.race,
    p.ethnicity,
    COUNT(DISTINCT c.patient) AS patient_count
FROM fact_conditions c
JOIN dim_patients p
  ON c.patient = p.patient_id
GROUP BY 
    c.code, c.description, p.age_group, p.gender, p.race, p.ethnicity;




-- COMMAND ----------

-- 💰 diagnosis_cost_view
-- Purpose: Report average and total costs per diagnosis group
CREATE OR REPLACE VIEW diagnosis_cost_view AS
SELECT 
    c.code AS diagnosis_code,
    c.description AS diagnosis_description,
    COUNT(DISTINCT c.encounter) AS num_encounters,
    COUNT(DISTINCT c.patient) AS num_patients,
    ROUND(SUM(e.total_claim_cost), 2) AS total_cost,
    ROUND(AVG(e.total_claim_cost), 2) AS avg_cost
FROM fact_conditions c
JOIN fact_encounters e
  ON c.encounter = e.id
GROUP BY c.code, c.description;



-- COMMAND ----------

-- 🏥 high_cost_patients_view
-- Purpose: Identify high-cost patients and their demographics
CREATE OR REPLACE VIEW high_cost_patients_view AS
SELECT 
    ps.patient_id,
    ps.total_claim_cost,
    p.gender,
    p.age_group,
    p.income,
    p.ethnicity,
    p.race
FROM fact_patient_summary ps
JOIN dim_patients p
  ON ps.patient_id = p.patient_id
WHERE ps.total_claim_cost >= 10000;



-- COMMAND ----------

-- 🔁 readmission_candidate_view
-- Purpose: Spot patients with multiple encounters within 30 days
CREATE OR REPLACE VIEW readmission_candidate_view AS
SELECT 
    e1.patient,
    e1.id AS first_encounter_id,
    e1.start AS first_encounter_start,
    e2.id AS second_encounter_id,
    e2.start AS second_encounter_start,
    DATEDIFF(e2.start, e1.stop) AS days_between
FROM fact_encounters e1
JOIN fact_encounters e2
  ON e1.patient = e2.patient AND e2.start > e1.stop
WHERE DATEDIFF(e2.start, e1.stop) <= 30;

-- COMMAND ----------

