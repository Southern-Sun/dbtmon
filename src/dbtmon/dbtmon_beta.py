import asyncio
import time
import re
import argparse


def _read_line(statement: str):
    if statement is None:
        return None

    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    statement = ansi_escape.sub("", statement)
    # Check if there's a timestamp (if not, it's a continuation of the previous line)
    try:
        int(statement.strip()[:2])
        timestamp = statement[:8]
        statement = statement[9:]
    except ValueError:
        return (statement,)

    # Check if it's a model status message
    if all(status not in statement for status in ["[RUN", "[ERROR", "[SUCCESS"]):
        return timestamp, statement

    message, status = statement.split("[")
    status = status[:-1]
    progress, _, total, *rest = message.split()
    text = " ".join(rest)
    match status.split():
        case "RUN":
            return timestamp, progress, total, text, "RUN"
        case ["ERROR", "in", runtime]:
            return timestamp, progress, total, text, "\033[31mERROR\033[0m", runtime
        case ["SUCCESS", code, "in", runtime]:
            return (
                timestamp,
                progress,
                total,
                text,
                "\033[32mSUCCESS\033[0m",
                runtime,
                code,
            )
        case _:
            return timestamp, progress, total, text, status


def _print_threads(threads: dict[int, dict], rewind: int):
    # print(threads)
    if rewind > 0:
        print(f"\033[{rewind+1}F")

    # We want success/error messages to appear at the top and not get overwritten
    completed_threads = {k: v for k, v in threads.items() if v["status"] != "RUN"}
    running_threads = {k: v for k, v in threads.items() if v["status"] == "RUN"}

    for thread in completed_threads.values():
        try:
            elapsed_time = float(thread["runtime"][:-1])
            formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
            hundredths = int(elapsed_time % 1 * 100)
            formatted_time = f"{formatted_time}.{hundredths:02}"
        except ValueError:
            formatted_time = thread["runtime"]
        print(
            thread["timestamp"],
            f"{thread['progress']} of {thread['total']}",
            thread["message"],
            f"[{thread['status']}",
            thread.get("code", "0"),
            f"in {formatted_time}]",
            " " * 5,
        )

    for thread in running_threads.values():
        elapsed_time = time.time() - thread["started_at"]
        formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        hundredths = int(elapsed_time % 1 * 100)
        formatted_time = f"{formatted_time}.{hundredths:02}"
        print(
            thread["timestamp"],
            f"{thread['progress']} of {thread['total']}",
            thread["message"],
            f"[ELAPSED: {formatted_time}]",
        )

    return len(running_threads)


def process_next_line(statement: str, threads: str, rewind: int):
    match _read_line(statement):
        case [continuation]:
            print(continuation)
        case [timestamp, statement]:
            print(timestamp, statement)
        case [timestamp, progress, total, message, status]:
            # New thread (status == "RUN")
            threads[progress] = {
                "timestamp": timestamp,
                "progress": progress,
                "total": total,
                "message": message,
                "status": status,
                "started_at": time.time(),
            }
            rewind = _print_threads(threads, rewind)
        case [timestamp, progress, total, message, status, runtime]:
            # Error on thread (status == "ERROR")
            if progress not in threads:
                raise ValueError(f"Thread {progress} not found")
            threads[progress].update(
                {
                    "timestamp": timestamp,
                    "message": message,
                    "status": status,
                    "runtime": runtime,
                }
            )
            rewind = _print_threads(threads, rewind)
            del threads[progress]
        case [timestamp, progress, total, message, status, runtime, code]:
            # Success on thread (status == "SUCCESS")
            if progress not in threads:
                raise ValueError(f"Thread {progress} not found")
            threads[progress].update(
                {
                    "timestamp": timestamp,
                    "message": message,
                    "status": status,
                    "runtime": runtime,
                    "code": code,
                }
            )
            rewind = _print_threads(threads, rewind)
            del threads[progress]
        case _:
            rewind = _print_threads(threads, rewind)

    return rewind


async def run(polling_rate: float = 0.2, minimum_wait: float = 0.025):
    threads = {}
    rewind = 0
    while True:
        input_task = asyncio.create_task(asyncio.to_thread(input))
        # Sleep for a moment to allow the task to complete if stdin is immediately ready
        await asyncio.sleep(minimum_wait)
        while not input_task.done():
            await asyncio.sleep(polling_rate)
            if not threads:
                continue
            _print_threads(threads, rewind)

        try:
            statement = await input_task
        except EOFError:
            return

        rewind = process_next_line(statement, threads, rewind)


parser = argparse.ArgumentParser()
parser.add_argument("--polling-rate", type=float, default=0.2)
parser.add_argument("--minimum-wait", type=float, default=0.025)

options = []
for action in parser._actions:
    options.extend(action.option_strings)

options = [option.strip("-") for option in options]

def main():
    args = parser.parse_args()
    asyncio.run(run(args.polling_rate, args.minimum_wait))


if __name__ == "__main__":
    main()
