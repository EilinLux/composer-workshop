"""
Chapter 06 Reference Test Suite: Airflow DAG Integrity & Governance Tests
=========================================================================
Reference Notebook: ../06_testing_and_cicd.ipynb
Runner Script:      ../scripts/01_run_dag_tests.sh
Chapter Guide:      ../README.md

What this test suite validates across all workshop chapters (01, 04, and 05):
  1. `test_no_import_errors`:
     Loads every DAG file using Airflow's `DagBag` and fails immediately if any
     file has a syntax error, missing dependency, or removed provider operator.
  2. `test_dags_have_tags_and_retries`:
     Enforces team governance standards: every DAG must have at least one tag,
     `catchup=False`, and `retries >= 1` in `default_args`.
  3. `test_no_cycles_in_dags`:
     Uses `airflow.utils.dag_cycle_tester.check_cycle` to verify that no DAG
     contains circular task dependencies.
"""

from __future__ import annotations

from pathlib import Path
import pytest

# `DagBag` is Airflow's internal parser that loads Python files and extracts DAG objects
from airflow.models import DagBag

# `check_cycle` verifies that a DAG is a true Directed Acyclic Graph (no loops)
from airflow.utils.dag_cycle_tester import check_cycle

# ------------------------------------------------------------------------------
# 1. Discover All Chapter `dags/` Directories
# ------------------------------------------------------------------------------
# Resolve the repository root (`composer-workshop/`) relative to this test file
REPO_ROOT = Path(__file__).resolve().parents[2]

# List every chapter directory that contains Airflow DAG files
DAG_DIRS = [
    REPO_ROOT / "01_airflow_fundamentals" / "dags",
    REPO_ROOT / "04_dataproc_serverless_etl" / "dags",
    REPO_ROOT / "05_deferrable_sensors_and_bigquery" / "dags",
]


# ------------------------------------------------------------------------------
# 2. Pytest Fixture: Load the `DagBag` Once per Test Module
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    """
    Collects all workshop DAGs across Chapters 01, 04, and 05 into a single DagBag.
    Setting `include_examples=False` ensures Airflow's built-in example DAGs are excluded.
    """
    # Initialize the DagBag with Chapter 01's DAG folder
    bag = DagBag(dag_folder=str(DAG_DIRS[0]), include_examples=False)

    # Collect additional DAGs from Chapters 04 and 05
    for d in DAG_DIRS[1:]:
        bag.collect_dags(
            dag_folder=str(d),
            only_if_updated=False,
            include_examples=False,
        )
    return bag


# ------------------------------------------------------------------------------
# 3. Unit Tests for DAG Integrity & Governance
# ------------------------------------------------------------------------------
def test_no_import_errors(dag_bag: DagBag) -> None:
    """
    Assert that every `.py` file in our `dags/` folders parsed without syntax
    errors, missing packages, or invalid operator imports.
    """
    assert not dag_bag.import_errors, (
        f"DAG import errors detected: {dag_bag.import_errors}"
    )


def test_dags_have_tags_and_retries(dag_bag: DagBag) -> None:
    """
    Enforce team engineering standards across every discovered DAG:
      - At least 3 workshop DAGs must be loaded (Chapters 01, 04, and 05).
      - Every DAG must define at least one UI tag (`dag.tags`).
      - Every DAG must disable historical backfill (`dag.catchup is False`).
      - Every DAG must configure at least 1 task retry (`retries >= 1`).
    """
    assert len(dag_bag.dags) >= 3, (
        f"Expected at least 3 workshop DAGs, found {len(dag_bag.dags)}: "
        f"{list(dag_bag.dags.keys())}"
    )

    for dag_id, dag in dag_bag.dags.items():
        # Verify UI tags are present for discoverability
        assert dag.tags, f"DAG '{dag_id}' must define at least one tag."
        # Verify catchup is disabled to prevent accidental historical backfills
        assert dag.catchup is False, f"DAG '{dag_id}' should set catchup=False."
        # Verify automatic task retries are configured
        retries = dag.default_args.get("retries", 0)
        assert retries >= 1, (
            f"DAG '{dag_id}' should configure at least 1 retry in default_args."
        )


def test_no_cycles_in_dags(dag_bag: DagBag) -> None:
    """
    Verify that no DAG contains a circular dependency loop (e.g., Task A >> Task B >> Task A).
    """
    for dag_id, dag in dag_bag.dags.items():
        check_cycle(dag)
