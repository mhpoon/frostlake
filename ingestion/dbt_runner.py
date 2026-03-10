"""
dbt_runner.py
-------------
Runs the frostlake dbt project programmatically using the dbtRunner API.

Run from the project root:
    python ingestion/dbt_runner.py

Optional commands:
    python ingestion/dbt_runner.py run
    python ingestion/dbt_runner.py test
    python ingestion/dbt_runner.py run --select silver
    python ingestion/dbt_runner.py run --select gold
"""

import sys
from dbt.cli.main import dbtRunner, dbtRunnerResult

DBT_PROJECT_DIR  = "dbt"
DBT_PROFILES_DIR = "dbt"


def run_dbt(command: str, *args: str) -> dbtRunnerResult:
    runner = dbtRunner()
    cli_args = [
        command,
        "--project-dir",  DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        *args,
    ]
    print(f"Running: dbt {' '.join(cli_args)}\n")
    result: dbtRunnerResult = runner.invoke(cli_args)

    if result.exception:
        print(f"\ndbt failed with exception: {result.exception}")
        sys.exit(1)

    return result


def main():
    # pass through any args from the command line
    # e.g. python ingestion/dbt_runner.py run --select silver
    extra_args = sys.argv[1:]
    command    = extra_args[0] if extra_args else "run"
    rest       = extra_args[1:] if len(extra_args) > 1 else []

    run_dbt(command, *rest)
    print("\ndbt run completed successfully.")


if __name__ == "__main__":
    main()