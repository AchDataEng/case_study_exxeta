"""
Entrypoint: run the medallion pipeline once.
  python -m src.main
  python -m src.main run

Orchestration (e.g. daily schedule) is done by Airflow
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from src.pipeline.run import run
    run()


if __name__ == "__main__":
    main()
