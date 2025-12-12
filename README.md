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
```

## 📡 Monitoring & Observability

- Airflow task retries, failure alerts, and SLA monitoring  
- CloudWatch logs for Glue jobs  
- Redshift WLM queue monitoring  
- Data Quality output stored in S3 for auditability  
- Logging & version control for all Glue scripts and Airflow DAGs  

---

## 🔐 IAM Roles

### **Airflow Role**
- Start Glue Jobs  
- Run Redshift SQL  
- Access S3 for reading and writing  

### **Glue Job Role**
- S3 read/write permissions  
- Glue Data Catalog access  
- CloudWatch logging  

### **Redshift Copy Role**
- S3 read-only access to processed data bucket  

---

## 🚀 Why This Pipeline Is Production Grade

- Modular and fully orchestrated workflow  
- Automated data quality validation  
- Schema cataloging and metadata versioning  
- Idempotent and safe to re-run  
- SCD Type 2 for historical data tracking  
- CI/CD ready for automated deployments  
- Enterprise-level monitoring and observability  

---

## 📚 Conclusion

This end-to-end pipeline demonstrates how to build a **real-world production workflow** for AWS-based Data Engineering systems using **Airflow, AWS Glue, PySpark, and Redshift**.  
It showcases best practices for orchestration, quality, transformation, and warehouse loading in a scalable, maintainable architecture.


