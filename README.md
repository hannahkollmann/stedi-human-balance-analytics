# stedi-human-balance-analytics

The STEDI Human Balance Platform collects customer, accelerometer, and step trainer data to help data scientists build balance-prediction machine learning models.

This project implements a full AWS Lakehouse ETL pipeline using:
	•	AWS S3
	•	AWS Glue (Python/Spark Jobs)
	•	AWS Glue Data Catalog
	•	AWS Athena

The purpose of the pipeline is to ingest raw data (Landing Zone), sanitize and filter it (Trusted Zone), and prepare curated datasets (Curated Zone) used for machine learning analytics.

This repository contains:
	•	SQL scripts for creating Landing Zone tables
	•	Python ETL scripts for AWS Glue jobs (Trusted + Curated layers)
	•	Screenshots showing successful Athena queries
	•	Original STEDI datasets

