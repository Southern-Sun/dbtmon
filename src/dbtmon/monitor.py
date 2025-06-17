import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable
from pathlib import Path

import networkx as nx

from dbtmon.config import DBTMonConfig
from dbtmon import __manifest_version__, __version__

COLOR_CONTROL_CHARS = [
    "\033[0m", # Reset
    "\033[31m", # Red
    "\033[32m", # Green
    "\033[33m", # Yellow
]


@dataclass
class DBTThread:
    timestamp: str
    progress: int
    total: int
    message: str
    status: str
    started_at: float
    runtime: float = None
    exit_code: int = 0
    min_concurrent_threads: int = 999
    max_concurrent_threads: int = 0
    blocking_started_at: float = None

    @property
    def model_name(self) -> str:
        *_, schema_model_name = self.message.strip(".").split()
        _, model_name = schema_model_name.split(".")
        return model_name

    @staticmethod
    def get_timestamp(value: float, format: str = "%H:%M:%S", display_ms: bool = True) -> str:
        """Takes a length of time and converts it to a timestamp, optionally with milliseconds"""
        formatted_time = time.strftime(format, time.gmtime(value))
        if not display_ms:
            return formatted_time

        hundredths = int(value % 1 * 100)
        return f"{formatted_time}.{hundredths:02}"

    def get_runtime(self) -> str:
        """Calculate and format the thread runtime"""
        elapsed_time = self.runtime or (time.time() - self.started_at)
        return self.get_timestamp(elapsed_time)

    def get_raw_blocking_time(self) -> float:
        """Get the amount of runtime for which the model was running alone"""
        try:
            return self.runtime - (self.blocking_started_at - self.started_at)
        except TypeError:
            # One of the values was not set -- the model could not have been blocking
            return 0.0
    
    def get_blocking_time(self) -> str:
        """Calculate and format the model blocking time"""
        return self.get_timestamp(self.get_raw_blocking_time())

    def get_status(self) -> str:
        """Get the formatted status of the thread"""
        match self.status:
            case "RUN":
                return "RUN"
            case "SUCCESS":
                return "\033[32mSUCCESS\033[0m"
            case "ERROR":
                return "\033[31mERROR\033[0m"
            case "SKIP":
                return "\033[33mSKIP\033[0m"
            case _:
                return "UNKNOWN"

    def __str__(self) -> str:
        stem = f"{self.timestamp} {self.progress} of {self.total} {self.message}"
        match self.status:
            case "RUN":
                return stem + f" [ELAPSED: {self.get_runtime()}]"
            case "SKIP":
                return stem + f" [{self.get_status()}]"
            case _:
                return (
                    stem
                    + f" [{self.get_status()} {self.exit_code}] in {self.get_runtime()}"
                )
            
    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "message": self.message,
            "status": self.status,
            "runtime": self.runtime,
            "exit_code": self.exit_code,
            "min_concurrent_threads": self.min_concurrent_threads,
            "max_concurrent_threads": self.max_concurrent_threads,
            "blocking_time": self.get_raw_blocking_time() if self.min_concurrent_threads == 1 else 0.0
        }


class DBTMonitor:
    def __init__(self, callback: Callable = None, **kwargs):
        self.config = DBTMonConfig(**kwargs)
        self.callback = callback
        self._threads = {}
        self.completed: list[DBTThread] = []
        self.rewind = 0

    @property
    def threads(self) -> dict[str, DBTThread]:
        return self._threads

    @property
    def running_threads(self) -> dict[str, DBTThread]:
        return {k: v for k, v in self.threads.items() if v.status == "RUN"}

    @property
    def completed_threads(self) -> dict[str, DBTThread]:
        return {k: v for k, v in self.threads.items() if v.status != "RUN"}

    def _print_threads(self):
        terminal_width = os.get_terminal_size().columns
        if self.rewind > 0:
            # This moves the cursor up in the terminal:
            print(f"\033[{self.rewind}F")

        # We want success/error messages to appear at the top and not get overwritten
        for thread in self.completed_threads.values():
            formatted_thread = str(thread)
            print(formatted_thread.ljust(terminal_width))

        # We need the running threads var twice so avoid recalculating it
        running_threads = self.running_threads.values()
        thread_count = len(running_threads)
        for thread in running_threads:
            formatted_thread = str(thread)
            print(formatted_thread.ljust(terminal_width))

            # Logging to detect blocking models
            if thread_count < thread.min_concurrent_threads:
                # Track when the thread gets to its lowest concurrent thread count
                thread.blocking_started_at = time.time()
            thread.min_concurrent_threads = min(thread.min_concurrent_threads, thread_count)
            thread.max_concurrent_threads = max(thread.max_concurrent_threads, thread_count)

        self.rewind = thread_count + 1

    def process_next_line(self, statement: str):
        if statement is None:
            return

        if not statement.startswith("\033[0m"):
            # This is a continuation of the previous line and never a job status message
            print(statement)
            return

        # Remove color control characters
        for char in COLOR_CONTROL_CHARS:
            statement = statement.replace(char, "")

        if all(
            status not in statement
            for status in ["[RUN", "[SUCCESS", "[ERROR", "[SKIP"]
        ):
            # This is not a model status message so we pass it through
            print(statement)
            return

        timestamp = statement[:8]
        full_message = statement[9:]
        message, status = full_message.split("[", maxsplit=1)

        # 1 of 5 START sql view model project.model_name ..........
        # 1 of 5 OK created sql view model project.model_name .....
        progress, _, total, *rest = message.split()
        progress, total = int(progress), int(total)
        text = " ".join(rest)

        match status.rstrip("]").split():
            case ["RUN"]:
                self.threads[progress] = DBTThread(
                    timestamp=timestamp,
                    progress=progress,
                    total=total,
                    message=text,
                    status="RUN",
                    started_at=time.time(),
                )
            case ["ERROR", "in", runtime]:
                if progress not in self.threads:
                    raise ValueError(f"Thread {progress} not found")
                self.threads[progress].timestamp = timestamp
                self.threads[progress].message = text
                self.threads[progress].status = "ERROR"
                self.threads[progress].runtime = float(runtime[:-1])
            case ["SUCCESS", code, "in", runtime]:
                if progress not in self.threads:
                    raise ValueError(f"Thread {progress} not found")
                self.threads[progress].timestamp = timestamp
                self.threads[progress].message = text
                self.threads[progress].status = "SUCCESS"
                self.threads[progress].runtime = float(runtime[:-1])
                self.threads[progress].exit_code = int(code)
            case ["SKIP"]:
                self.threads[progress] = DBTThread(
                    timestamp=timestamp,
                    progress=progress,
                    total=total,
                    message=text,
                    status="SKIP",
                    started_at=None,
                )
            case _:
                print(f"Unknown status: '{status}'")

        self._print_threads()
        if self.threads[progress].status == "RUN":
            return

        # Archive completed threads
        self.completed.append(self.threads[progress])
        del self.threads[progress]

    def get_project_dir(self) -> Path:
        # Check command line args first
        possible_directories: list[Path] = []
        if self.config.dbtmon_project_dir is not None:
            possible_directories.append(Path(self.config.dbtmon_project_dir))

        # Check environment variables
        if "DBT_PROJECT_DIR" in os.environ:
            possible_directories.append(Path(os.environ.get("DBT_PROJECT_DIR")))

        # Find the directory otherwise
        cwd = Path.cwd()
        possible_directories.extend([cwd, *cwd.parents])
        
        for directory in possible_directories:
            if (directory / "dbt_project.yml").is_file():
                return directory

        # Report failure to find. First directory in the list should be the best
        raise FileNotFoundError(f"Unable to find dbt_project.yml at {possible_directories[0]}")
    
    def print_blocking_threads(self) -> None:
        if len(self.completed) < self.config.blocking_minimum_job_size:
            return
        
        for thread in self.completed:
            if thread.min_concurrent_threads != 1:
                continue

            if thread.get_raw_blocking_time() < self.config.minimum_blocking_time:
                continue

            print(
                "[dbtmon]",
                "Blocking Model:",
                f"name={thread.model_name}",
                f"build_time={thread.get_runtime()}",
                f"blocking_time={thread.get_blocking_time()}"
            )

    def build_manifest(self) -> None:
        """
        Builds or updates the dbtmon_manifest.json file. File is placed in the target/ directory
        by default but this can be overridden in the settings.

        The manifest is a JSON file like the following python dict:
        {
            "version": __version__,
            "last_run": ["model_alias_1", "model_alias_2", ...],
            "nodes": {"model_alias_1": {"DBTThread.to_dict()"}, ...}
        }
        """
        try:
            project_dir = self.get_project_dir()
        except FileNotFoundError as e:
            print(e)
            return

        # Custom manifest location?
        manifest_path = Path(self.config.dbtmon_manifest_path)
        if Path(".") in manifest_path.parents:
            # This is a relative path
            manifest_path = project_dir / manifest_path

        if not manifest_path.parent.is_dir():
            print("Error: Invalid manifest location!")
            return

        manifest = {
            "version": __manifest_version__,
            "last_run": [],
            "nodes": {}
        }
        if manifest_path.is_file():
            with open(manifest_path, "r", encoding="utf-8") as f:
                existing_manifest = json.load(f)

            if existing_manifest["version"] == __manifest_version__:
                # Existing manifest is up to date, keep its data
                manifest["nodes"].update(existing_manifest["nodes"])

        manifest["nodes"].update(
            {thread.model_name: thread.to_dict() for thread in self.completed}
        )
        manifest["last_run"] = [thread.model_name for thread in self.completed]

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        return manifest
    
    def get_dbt_dag(self, dbtmon_manifest: dict = None) -> nx.DiGraph:
        dbtmon_manifest = dbtmon_manifest or {}
        try:
            project_dir = self.get_project_dir()
        except FileNotFoundError as e:
            print(e)
            return
        
        dbt_manifest_path = project_dir / "target" / "manifest.json"

        if not dbt_manifest_path.is_file():
            print(f"Error: Unable to find dbt manifest at {dbt_manifest_path}")
            return
        
        with open(dbt_manifest_path, "r") as f:
            dbt_manifest: dict[str, dict[str, dict]] = json.load(f)

        all_nodes = {**dbt_manifest["nodes"], **dbt_manifest.get("sources", {})}

        dag = nx.DiGraph()
        for node_name, node_data in all_nodes.items():
            dag.add_node(node_name, **node_data)

            # Find the entry in dbtmon_manifest for edge weighting
            # dbtmon stores this by name, not the fully qualified name
            # int__my_model vs my_project.model.int__my_model
            dbtmon_data = dbtmon_manifest.get(node_data.get("name"), {})
            if dbtmon_data:
                print(f"Found {node_name} in manifest!")

            for dependency in node_data.get("depends_on", {}).get("nodes", []):
                dag.add_edge(dependency, node_name, **dbtmon_data)

        return dag

    async def run(self):
        while True:
            input_task = asyncio.create_task(asyncio.to_thread(input))
            await asyncio.sleep(self.config.minimum_wait)
            while not input_task.done():
                await asyncio.sleep(self.config.polling_rate)
                if not self.threads:
                    continue
                self._print_threads()

            try:
                statement = await input_task
            except EOFError:
                break

            self.process_next_line(statement)

        # Post-dbt work
        # Identify blocking models
        if not self.config.disable_blocking_thread_detection:
            self.print_blocking_threads()

        # Create or update dbtmon_manifest.json
        if not self.config.disable_dbtmon_manifest:
            manifest = self.build_manifest()

            # Calculate Critical Path
            dag = self.get_dbt_dag(dbtmon_manifest=manifest)
            critical_path = nx.dag_longest_path(dag, weight="runtime", default_weight=0)
            for node in critical_path:
                *_, model_name = node.split(".", maxsplit=2)
                runtime = manifest.get(model_name, {}).get("runtime")
                if runtime is None:
                    timestamp = "N/A"
                else:
                    timestamp = DBTThread.get_timestamp(runtime)
                print(f"{node}: {timestamp}")

        if self.callback is None:
            return
        self.callback()

    def run_file(self, filename: str):
        with open(filename, "r") as file:
            for line in file:
                self.process_next_line(line.strip())

                if not self.threads:
                    continue
                for _ in range(5):
                    time.sleep(self.config.polling_rate)
                    self._print_threads()


if __name__ == "__main__":
    monitor = DBTMonitor()
    try:
        # asyncio.run(monitor.run_async())
        monitor.run_file("tests/test_output.txt")
    except KeyboardInterrupt:
        print("\nProcess terminated by user.")
