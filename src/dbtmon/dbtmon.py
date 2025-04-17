import threading
import time
import re
import argparse


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
OPTIONS = []
for action in parser._actions:
    OPTIONS.extend(action.option_strings)

OPTIONS = [option.strip("-") for option in OPTIONS]


class DBTMonitor:
    def __init__(self, polling_rate: float = 0.2, minimum_wait: float = 0.025):
        self.polling_rate = polling_rate
        self.minimum_wait = minimum_wait
        self.threads = {}
        self.rewind = 0

    def _print_threads(self, threads, rewind):
        # Placeholder for thread printing logic
        return rewind

    def process_next_line(self, statement):
        # Placeholder for processing logic
        return self.rewind

    def run(self):
        while True:
            input_task = threading.Thread(target=input)
            input_task.start()
            time.sleep(self.minimum_wait)
            while input_task.is_alive():
                time.sleep(self.polling_rate)
                if not self.threads:
                    continue
                self._print_threads(self.threads, self.rewind)

            try:
                statement = input_task.join()
            except EOFError:
                return

            self.rewind = self.process_next_line(statement)