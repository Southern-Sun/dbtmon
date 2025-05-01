import argparse
import asyncio
import subprocess
import sys
import yaml
from pathlib import Path

from dbtmon.monitor import DBTMonitor


# Define command line arguments
parser = argparse.ArgumentParser(description="dbt monitor")
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

    dbtmon_args = []
    dbtmon_config = Path.home() / ".dbt" / "dbtmon.yml"
    if dbtmon_config.exists():
        with open(dbtmon_config, "r") as f:
            config: dict = yaml.safe_load(f)

        for key, value in config.items():
            if key not in OPTIONS:
                print(
                    f"Warning: Unknown config option '{key}' in {dbtmon_config}",
                    file=sys.stderr,
                )
                continue

            dbtmon_args.append(f"--{key}")
            if value is not None:
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
