Overview
========

Welcome to the Data Warehouse and Analytics Project repository! 🚀

This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.This projects walks through end to end data engineering works to build the complete data warehouse ready for business use.

This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Overview
================

1. **Data Architecture:** Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
2. **ETL Pipelines:** Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling:** Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting:** Creating SQL-based reports and dashboards for actionable insights.

Project Contents
================

The Astro project contains the following files and folders:

- **dags:** This folder contains the Python files for your Airflow DAGs. 
- **Dockerfile:** This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- **include:** This folder contains any additional files that you want to include as part of your project. It is empty by default.
- **packages.txt:** Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- **requirements.txt:** Install Python packages needed for your project by adding them to this file. It is empty by default.
- **plugins:** Add custom or community plugins for your project to this file. It is empty by default.
- **airflow_settings.yaml:** Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Before starting Airflow on your local machine, create a virtual environment and run the following commands:
1. `curl -sSL install.astronomer.io | sudo bash -s` (to install the Astronomer CLI)
2. `astro dev init` (to initialize the astro project)

Then start Airflow on your local machine by running `astro dev start`.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Data Architecture
=================================

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.

- **Bronze Layer:** Stores raw data as-is from the source systems. Data is ingested from CSV Files into the BigQuery.
- **Silver Layer:** This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
- **Gold Layer:** Houses business-ready data modeled into a star schema required for reporting and analytics.

Project Requirements
=================================

***Building the Data Warehouse (Data Engineering)***

***Objective***:

Develop a modern data warehouse using BigQuery to consolidate sales data, enabling analytical reporting and informed decision-making.

**Specifications:**
- Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
- Data Quality: Cleanse and resolve data quality issues prior to analysis.
- Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
- Scope: Focus on the latest dataset only; historization of data is not required.

BI: Analytics & Reporting (Data Analysis)
=================================

***Objective***:

Develop SQL-based analytics to deliver detailed insights into:

- Customer Behavior
- Product Performance
- Sales Trends
- These insights empower stakeholders with key business metrics, enabling strategic decision-making.