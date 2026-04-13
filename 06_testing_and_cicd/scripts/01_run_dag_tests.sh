#!/usr/bin/env bash
# ==============================================================================
# Chapter 06 — Script 01: Run Local DAG Integrity Tests with uv & pytest
# Reference Notebook: ../06_testing_and_cicd.ipynb
# Reference Tests:    ../tests/test_dag_integrity.py
#
# What this script does:
#   1. Synchronizes Python dependencies defined in `pyproject.toml` via `uv sync`.
#   2. Runs the `pytest` DAG integrity suite across Chapters 01, 04, and 05.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CHAPTER_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "==> [1/2] Syncing Python virtual environment with uv..."
uv sync

echo "==> [2/2] Running Airflow DAG integrity tests across all chapters..."
uv run pytest 06_testing_and_cicd/tests/test_dag_integrity.py -v
