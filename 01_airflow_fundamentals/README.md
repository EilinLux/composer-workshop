# Chapter 01 — Apache Airflow Fundamentals

## Overview

In this chapter, we introduce the core concepts of **workflow orchestration** and **Apache Airflow 2.x**—the open-source engine that powers Google Cloud Composer.

### What You Will Learn
1. **Data vs. Workflow Orchestration**: Why modern data engineering and ML pipelines require a central orchestrator instead of fragile `cron` scripts.
2. **Apache Airflow Architecture**: How the **Scheduler**, **DAG Processor**, **Metadata Database**, **Executor**, **Workers**, and **Web Server** interact.
3. **Defining Task Dependencies**: Bitshift operators (`>>`, `<<`), `chain()`, and `cross_downstream()`.
4. **The Evolution to `@dag` and the TaskFlow API (`@task`)**: Moving from verbose boilerplate to clean, functional Python pipelines with automatic XCom data passing.
5. **Scheduling, Monitoring, Retries & Environment Variables**: Making pipelines resilient and configurable.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`01_airflow_fundamentals.ipynb`](./01_airflow_fundamentals.ipynb) | Interactive notebook covering Airflow architecture, dependency patterns, `@dag`/`@task`, retries, and environment variables. |
| [`dags/taskflow_basics_dag.py`](./dags/taskflow_basics_dag.py) | Heavily commented reference DAG demonstrating `@dag`, `@task` (TaskFlow API), `BashOperator`, retries, and environment variables. |

---
