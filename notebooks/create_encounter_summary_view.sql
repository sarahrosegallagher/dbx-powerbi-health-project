-- Databricks notebook source
CREATE OR REPLACE VIEW encounter_summary_view AS
SELECT
    f.patient,
    f.year,
    f.month,
    f.encounterclass,
    f.state as org_state,
    f.org_zip,
    f.age_at_encounter,
    d.gender,
    d.income,
    d.race,
    d.ethnicity,
    COUNT(DISTINCT f.id) AS num_encounters,
    ROUND(SUM(f.total_claim_cost), 2) AS total_cost,
    ROUND(AVG(f.total_claim_cost), 2) AS avg_cost,
    ROUND(MAX(f.total_claim_cost), 2) AS max_cost,
    ROUND(MIN(f.total_claim_cost), 2) AS min_cost,
    ROUND(AVG(f.encounter_duration_minutes), 1) AS avg_duration_minutes
FROM fact_encounters f
LEFT JOIN dim_patients d
  ON f.patient = d.patient_id
GROUP BY 
    f.patient,
    f.year,
    f.month,
    f.encounterclass,
    f.state,
    f.org_zip,
    f.age_at_encounter,
    d.gender,
    d.income,
    d.race,
    d.ethnicity
