# End-to-End Data Engineering Pipeline Using Airflow, AWS Glue, Redshift & PySpark

This project demonstrates a production-grade data engineering pipeline orchestrated using **Apache Airflow**, with scalable data processing using **AWS Glue (PySpark)** and downstream loading into **Amazon Redshift** using staging + **SCD Type 2** merge logic.

It showcases key enterprise data engineering capabilities:

- Event-driven orchestration using Airflow  
- Schema discovery using Glue Crawler  
- Automated Data Quality Checks (Deequ)  
- Scalable PySpark transformation jobs  
- Redshift COPY into staging  
- SCD Type 2 merging into final tables  
- End-to-end monitoring & reliability

---

## 📌 Architecture Overview

```text
S3 (Landing Zone)
      ↓   (S3PrefixSensor)
Airflow DAG Trigger
      ↓
AWS Glue Crawler → Updates Glue Data Catalog
      ↓
AWS Glue Data Quality Job (PySpark + Deequ)
      ↓   (Airflow Python validation)
AWS Glue Transformation Job (PySpark)
      ↓
S3 (Processed Zone)
      ↓
Redshift COPY → Staging Table
      ↓
Redshift MERGE → Target Table (SCD Type 2)
