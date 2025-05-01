import asyncio
import os
import time
from dataclasses import dataclass
from typing import Callable


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

    def get_runtime(self) -> float:
        """Calculate and format the thread runtime"""
        elapsed_time = self.runtime or (time.time() - self.started_at)
        formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        hundredths = int(elapsed_time % 1 * 100)
        return f"{formatted_time}.{hundredths:02}"

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


class DBTMonitor:
    def __init__(
        self,
        polling_rate: float = 0.2,
        minimum_wait: float = 0.025,
        callback: Callable = None,
    ):
        self.polling_rate = polling_rate
        self.minimum_wait = minimum_wait
        self.callback = callback
        self._threads = {}
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
        for thread in (running_threads := self.running_threads.values()):
            formatted_thread = str(thread)
            print(formatted_thread.ljust(terminal_width))

        self.rewind = len(running_threads) + 1

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

        # Prune completed threads
        del self.threads[progress]

    async def run(self):
        while True:
            input_task = asyncio.create_task(asyncio.to_thread(input))
            await asyncio.sleep(self.minimum_wait)
            while not input_task.done():
                await asyncio.sleep(self.polling_rate)
                if not self.threads:
                    continue
                self._print_threads()

            try:
                statement = await input_task
            except EOFError:
                break

            self.process_next_line(statement)

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
                    time.sleep(self.polling_rate)
                    self._print_threads()


if __name__ == "__main__":
    monitor = DBTMonitor()
    try:
        # asyncio.run(monitor.run_async())
        monitor.run_file("tests/test_output.txt")
    except KeyboardInterrupt:
        print("\nProcess terminated by user.")
