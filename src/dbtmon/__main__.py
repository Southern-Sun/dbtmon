import argparse
import asyncio
import subprocess
import sys
from typing import Collection
import yaml
from pathlib import Path

from dbtmon.monitor import DBTMonitor
from dbtmon import __version__

# Define command line arguments
parser = argparse.ArgumentParser(description="dbt monitor")
parser.add_argument(
    "--version",
    action="version",
    version=__version__
)
parser.add_argument(
    "--polling-rate",
    type=float,
    default=0.2,
    help="Polling rate for checking stdin (default: 0.2)",
)
parser.add_argument(
    "--minimum-wait",
    type=float,
    default=0.025,
    help="Minimum wait time before checking stdin (default: 0.025)",
)
parser.add_argument(
    "--dbtmon-project-dir",
    type=str,
    default=None,
    help="Path to the dbtmon project directory (default: None)",
)

# Blocking Thread Detection
parser.add_argument(
    "--disable-blocking-thread-detection",
    action="store_true",
    help="Disable blocking thread detection (default: False)",
)
parser.add_argument(
    "--minimum-blocking-time",
    type=float,
    default=60.0,
    help="Minimum time (in seconds) for a thread to be considered blocking (default: 60.0)",
)
parser.add_argument(
    "--blocking-minimum-job-size",
    type=int,
    default=0,
    help="Minimum number of jobs in flight to trigger blocking detection (default: 0)",
)

# dbtmon Manifest Controls
parser.add_argument(
    "--disable-dbtmon-manifest",
    action="store_true",
    help="Disable generation and usage of the dbtmon manifest (default: False)",
)
parser.add_argument(
    "--dbtmon-manifest-path",
    type=str,
    default="target/dbtmon_manifest.json",
    help="Path to the dbtmon manifest file (default: target/dbtmon_manifest.json)",
)

# Provide a list of CLI options to export
OPTIONS: list[str] = []
for action in parser._actions:
    OPTIONS.extend(action.option_strings)

OPTIONS = [option.lstrip("-") for option in OPTIONS]


def pipe():
    args = parser.parse_args()
    monitor = DBTMonitor(polling_rate=args.polling_rate, minimum_wait=args.minimum_wait)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        print("\nProcess terminated by user.")
        sys.exit(0)


def cli():
    if len(sys.argv) == 2 and sys.argv[1] in {"--help", "-h", "--version"}:
        # Pass these flags to the internal pipe directly
        subprocess.run(["__dbtmonpipe__"] + sys.argv[1:])
        return

    # TODO: Improve this flow so that CLI args can be passed from outside the config file

    dbtmon_args = []
    # Handle custom project directory settings
    try:
        directory_index = sys.argv.index("--project-dir")
        dbtmon_args = ["--dbtmon_project_dir", sys.argv[directory_index+1]]
    except ValueError:
        # project-dir is not specified in the args
        pass

    dbtmon_config = Path.home() / ".dbt" / "dbtmon.yml"
    if dbtmon_config.exists():
        with open(dbtmon_config, "r") as f:
            config: dict = yaml.safe_load(f) or {}

        for key, value in config.items():
            if key not in OPTIONS:
                print(
                    f"Warning: Unknown config option '{key}' in {dbtmon_config}",
                    file=sys.stderr,
                )
                continue

            dbtmon_args.append(f"--{key}")
            if value is None:
                continue

            if isinstance(value, Collection):
                dbtmon_args.extend(value)
            else:
                dbtmon_args.append(value)

    try:
        # Run `dbt` with user args, pipe stdout into __dbtmonpipe__
        dbt = subprocess.Popen(
            ["dbt"] + sys.argv[1:],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=1,
            universal_newlines=True,
        )

        # __dbtmonpipe__ is installed as an entry point
        dbtmon = subprocess.Popen(
            ["__dbtmonpipe__"] + dbtmon_args,
            stdin=dbt.stdout,
        )

        dbt.stdout.close()
        dbtmon.communicate()

    except FileNotFoundError as e:
        print(f"Error running command: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    pipe()
