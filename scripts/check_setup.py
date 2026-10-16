"""Day 1 setup check. Everyone runs this from the repo root:

    python scripts/check_setup.py           # everything except Kafka
    python scripts/check_setup.py --kafka   # also checks Kafka is running

Each line says OK or FIX, with what to do. Nothing secret is printed.
"""

from __future__ import annotations

import argparse
import importlib
import platform
import re
import shutil
import socket
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

results: list[bool] = []


def report(ok: bool, label: str, detail: str = "") -> None:
    results.append(ok)
    mark = "OK " if ok else "FIX"
    print(f"[{mark}] {label}" + (f"  ({detail})" if detail else ""))


def check_python() -> None:
    v = sys.version_info
    report(v >= (3, 10), "Python 3.10 or newer", f"you have {v.major}.{v.minor}")


def check_os() -> None:
    system = platform.system()
    if system == "Windows":
        report(
            False,
            "Running inside WSL (Ubuntu), not plain Windows",
            "Spark on plain Windows needs extra hacks. Open Ubuntu (WSL) and run this there. See README.",
        )
    else:
        report(True, "Operating system", f"{system}")


def check_java() -> None:
    java = shutil.which("java")
    if not java:
        report(False, "Java installed", "install Java 17, see README")
        return
    out = subprocess.run([java, "-version"], capture_output=True, text=True)
    text = out.stderr + out.stdout
    match = re.search(r'version "(\d+)', text)
    major = int(match.group(1)) if match else 0
    if major == 1:  # old style "1.8"
        major = 8
    if major == 17:
        report(True, "Java 17", f"you have Java {major}")
    else:
        # Tested: on Java 21, Spark 3.5's Arrow/pandas conversion crashes (toPandas, Streamlit, charts).
        report(False, "Java 17", f"you have Java {major}; install Java 17 and set JAVA_HOME to it (see README)")


def check_packages() -> None:
    needed = {
        "pyspark": "pyspark",
        "pandas": "pandas",
        "pyarrow": "pyarrow",
        "confluent_kafka": "confluent-kafka",
        "requests": "requests",
        "dotenv": "python-dotenv",
        "streamlit": "streamlit",
        "matplotlib": "matplotlib",
        "setuptools": "setuptools",  # Spark 3.5 needs it on Python 3.12+ (distutils)
    }
    missing = []
    for module, pip_name in needed.items():
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(pip_name)
    report(not missing, "Python packages", "run: pip install -r requirements.txt" if missing else "all installed")


def check_env() -> None:
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        report(False, ".env file", "copy .env.example to .env")
        return
    from common import config

    report(True, ".env file", "found")
    if config.OPENAQ_API_KEY:
        report(True, "OpenAQ API key set")
    else:
        print("[ - ] OpenAQ API key not set  (only Role 1 and the demo laptop need it)")


def check_spark() -> None:
    try:
        from common.spark import get_spark

        spark = get_spark("setup_check")
        rows = spark.range(1_000_000).selectExpr("sum(id) AS s").collect()
        ok = rows[0]["s"] == 499_999_500_000
        version = spark.version
        spark.stop()
        report(ok, "Spark runs a job", f"Spark {version}")
    except Exception as exc:  # show the first line only
        report(False, "Spark runs a job", str(exc).splitlines()[0][:160])


def check_docker() -> None:
    report(bool(shutil.which("docker")), "Docker installed", "needed to run Kafka")


def check_kafka() -> None:
    from common.config import KAFKA_BOOTSTRAP

    host, port = KAFKA_BOOTSTRAP.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=3):
            pass
    except OSError:
        report(False, f"Kafka reachable at {KAFKA_BOOTSTRAP}", "run: docker compose -f infra/docker-compose.yml up -d")
        return
    try:
        from confluent_kafka.admin import AdminClient

        topics = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}).list_topics(timeout=5).topics
        from common.config import KAFKA_TOPIC

        report(KAFKA_TOPIC in topics, f"Kafka topic '{KAFKA_TOPIC}' exists", "" if KAFKA_TOPIC in topics else "check the create-topic container logs")
    except Exception as exc:
        report(False, "Kafka topic check", str(exc).splitlines()[0][:160])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kafka", action="store_true", help="also check that Kafka is running")
    parser.add_argument("--skip-spark", action="store_true", help="skip the Spark test (it takes ~20 s)")
    args = parser.parse_args()

    print("AirGuard setup check\n")
    check_python()
    check_os()
    check_java()
    check_packages()
    check_env()
    check_docker()
    if not args.skip_spark:
        check_spark()
    if args.kafka:
        check_kafka()

    from common.config import ensure_dirs

    ensure_dirs()
    failed = results.count(False)
    print(f"\n{len(results) - failed} OK, {failed} to fix.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
