# Chapter 02 — Google Cloud Composer Architecture (v2 vs. v3)

## Overview

In this chapter, we explore how **Google Cloud Composer** provisions and manages a production-grade Apache Airflow environment on Google Cloud Platform, and how **Cloud Composer 3** simplifies networking and infrastructure management compared to **Cloud Composer 2**.

### What You Will Learn
1. **Managed Airflow Components on GCP**:
   * **Cloud Storage Bucket**: Stores your `/dags`, `/plugins`, and `/data` folders and syncs them automatically to workers and schedulers.
   * **Cloud SQL**: Managed relational database backing the Airflow Metadata Database.
   * **Compute Layer (GKE / Managed Tenant Infrastructure)**: Runs Airflow Schedulers, Workers, Triggerers, DAG Processors, and the Airflow Web Server.
   * **Cloud Logging & Cloud Monitoring**: Native observability for task logs, scheduler heartbeats, and resource utilization.
2. **Cloud Composer 2 vs. Cloud Composer 3**:
   * Why **Composer 2** requires a VPC-native GKE cluster with secondary IP ranges for Pods and Services.
   * How **Composer 3** eliminates GKE IP address exhaustion and secondary subnet range management by running infrastructure in a Google-managed tenant project connected via **Private Service Connect (PSC)**.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`02_cloud_composer_architecture.ipynb`](./02_cloud_composer_architecture.ipynb) | Interactive notebook detailing Cloud Composer's managed architecture and comparing Composer 2 vs. Composer 3. |

---

