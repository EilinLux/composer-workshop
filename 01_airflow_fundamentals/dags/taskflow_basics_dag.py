"""
Chapter 01 Reference DAG: Apache Airflow Fundamentals & TaskFlow API
====================================================================
Reference Notebook: ../01_airflow_fundamentals.ipynb
Chapter Guide:      ../README.md

Purpose:
  Demonstrates modern Apache Airflow 2.x DAG authoring best practices:
  1. Using `@dag` to declare a workflow as a standard Python function.
  2. Using `@task` (TaskFlow API) to convert Python functions into Airflow tasks
     with automatic XCom serialization and dependency tracking.
  3. Combining traditional Operators (`BashOperator`) with TaskFlow (`@task`) functions.
  4. Configuring task retries, exponential backoff, and runtime environment variables.
"""

# Enable postponed evaluation of type annotations (PEP 563) for cleaner type hints
from __future__ import annotations

# Standard library imports for reading environment variables
import os

# Pendulum is Airflow's recommended timezone-aware datetime library
import pendulum

# Import the modern @dag and @task decorators introduced in Apache Airflow 2.0
from airflow.decorators import dag, task

# Import BashOperator to demonstrate mixing traditional Operators with @task functions
from airflow.operators.bash import BashOperator

# ------------------------------------------------------------------------------
# 1. Default Task Arguments (`DEFAULT_ARGS`)
# ------------------------------------------------------------------------------
# These arguments are inherited by every task inside the DAG unless a specific
# task overrides them (for example, `@task(retries=3)` below).
DEFAULT_ARGS = {
    # Logical owner of the pipeline displayed in the Airflow Web UI
    "owner": "data-engineering",
    # Number of times a task should automatically retry if it fails
    "retries": 2,
    # Base wait duration before the first retry attempt
    "retry_delay": pendulum.duration(minutes=2),
    # Doubles the wait time between subsequent retries to avoid overwhelming APIs
    "retry_exponential_backoff": True,
}


# ------------------------------------------------------------------------------
# 2. Workflow Definition (`@dag` Decorator)
# ------------------------------------------------------------------------------
@dag(
    # Unique identifier for this DAG across the entire Airflow / Composer environment
    dag_id="ch01_taskflow_fundamentals_example",
    # Timestamp from which the scheduler starts calculating logical execution intervals
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Cron preset: runs once a day at 00:00 UTC after each 24-hour data interval closes
    schedule="@daily",
    # When False, prevents Airflow from launching historical backfill runs since start_date
    catchup=False,
    # Apply the default retry and ownership settings defined above to all tasks
    default_args=DEFAULT_ARGS,
    # UI labels used to filter and group DAGs in the Airflow Web interface
    tags=["chapter-01", "fundamentals", "taskflow"],
)
def taskflow_fundamentals_pipeline():
    """End-to-end educational pipeline illustrating TaskFlow API and operator chaining."""

    # --------------------------------------------------------------------------
    # Task 1: Traditional Operator (`BashOperator`)
    # --------------------------------------------------------------------------
    # Executes a shell command on the Airflow worker.
    # Notice `{{ ds }}`: Airflow renders this Jinja macro at runtime with the
    # logical execution date formatted as YYYY-MM-DD.
    announce_pipeline_start = BashOperator(
        task_id="announce_pipeline_start",
        bash_command='echo "Starting pipeline for logical date {{ ds }} in env: $WORKSHOP_ENV"',
        # Pass environment variables specifically scoped to this subprocess
        env={"WORKSHOP_ENV": os.getenv("WORKSHOP_ENV", "development")},
    )

    # --------------------------------------------------------------------------
    # Task 2: TaskFlow Producer (`@task`)
    # --------------------------------------------------------------------------
    # Any JSON-serializable value returned by a `@task` function is automatically
    # persisted in Airflow's Metadata Database (XCom) and passed to downstream tasks.
    @task
    def extract_sample_orders() -> list[dict[str, float | str]]:
        """Simulates extracting raw order records from an upstream API or operational database."""
        return [
            {"order_id": "ORD-101", "product_id": "PROD-A1", "amount": 120.50},
            {"order_id": "ORD-102", "product_id": "PROD-B2", "amount": 85.00},
            {"order_id": "ORD-103", "product_id": "PROD-A1", "amount": 49.50},
        ]

    # --------------------------------------------------------------------------
    # Task 3: TaskFlow Transformer (`@task` with custom retry override)
    # --------------------------------------------------------------------------
    # Overrides the DAG's default `retries=2` with `retries=3` for this specific task.
    # Accepts `orders` directly from the upstream `extract_sample_orders` XCom output.
    @task(retries=3)
    def compute_order_metrics(orders: list[dict[str, float | str]]) -> dict[str, float | int]:
        """Aggregates total revenue and order count from the extracted order list."""
        # Sum the 'amount' field across all order dictionaries
        total_revenue = round(sum(float(item["amount"]) for item in orders), 2)
        return {
            "order_count": len(orders),
            "total_revenue": total_revenue,
        }

    # --------------------------------------------------------------------------
    # Task 4: TaskFlow Consumer (`@task`)
    # --------------------------------------------------------------------------
    @task
    def log_summary(metrics: dict[str, float | int]) -> None:
        """Prints the aggregated metrics so they appear in Cloud Logging / Airflow UI."""
        print(
            f"Processed {metrics['order_count']} orders "
            f"with total revenue: EUR {metrics['total_revenue']:.2f}"
        )

    # --------------------------------------------------------------------------
    # 3. Task Invocation & Dependency Graph Wiring
    # --------------------------------------------------------------------------
    # Invoking `@task` functions passes XCom references (`XComArg`) and automatically
    # registers the dependency chain: `extract_sample_orders >> compute_order_metrics >> log_summary`.
    raw_orders = extract_sample_orders()
    summary_metrics = compute_order_metrics(raw_orders)
    final_log = log_summary(summary_metrics)

    # Use the bitshift operator (`>>`) to ensure `announce_pipeline_start` completes
    # before `extract_sample_orders` begins executing.
    announce_pipeline_start >> raw_orders
    final_log


# Instantiate the `@dag`-decorated function so Airflow's DAG File Processor registers it
taskflow_fundamentals_pipeline()
