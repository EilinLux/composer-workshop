# Chapter 06 — Unit Testing DAGs with `pytest` & CI/CD Deployment

## Overview

Because **Google Cloud Composer** continuously parses Python files uploaded to `gs://<composer-bucket>/dags/`, a syntax error, broken import, deprecated operator parameter, or cyclic dependency can break DAG scheduling in production.

In this chapter, we validate every DAG in the workshop locally using **`uv`** and **`pytest`** (`DagBag`) before syncing files to Cloud Storage.

### What We Are Going to Do
1. **Load All Workshop DAGs into Airflow's `DagBag`**:
   * Collect all DAGs across Chapters `01`, `04`, and `05` (`include_examples=False`).
   * Assert that `dag_bag.import_errors` is completely empty.
2. **Enforce Engineering & Governance Standards**:
   * Verify every DAG defines organizational `tags`.
   * Verify every DAG configures `retries >= 1` in `default_args`.
   * Verify every DAG has `catchup=False`.
3. **Detect Cyclic Dependencies**:
   * Run Airflow's `check_cycle(dag)` across every discovered DAG.
4. **Automate Deployment with Cloud Build / GitHub Actions**:
   * Run `uv sync --frozen && uv run pytest` in CI before running `gcloud storage rsync` to the Composer DAGs bucket.

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`06_testing_and_cicd.ipynb`](./06_testing_and_cicd.ipynb) | Interactive notebook explaining `DagBag` unit tests, governance checks, and Cloud Build CI/CD pipelines. |
| [`tests/test_dag_integrity.py`](./tests/test_dag_integrity.py) | Heavily commented `pytest` suite testing all DAGs across Chapters `01`, `04`, and `05`. |
| [`scripts/01_run_dag_tests.sh`](./scripts/01_run_dag_tests.sh) | Syncs the `uv` virtual environment and runs the `pytest` DAG integrity test suite. |

---

## How to Run the Script

From the repository root:

```bash
bash 06_testing_and_cicd/scripts/01_run_dag_tests.sh
```
