from dataclasses import dataclass

@dataclass
class DBTMonConfig:
    # Runtime parameters
    polling_rate: float = 0.2
    minimum_wait: float = 0.025
    dbtmon_project_dir: str = None

    # Blocking Thread Detection
    disable_blocking_thread_detection: bool = False
    minimum_blocking_time: float = 60.0
    blocking_minimum_job_size: int = 0

    # dbtmon Manifest Controls
    disable_dbtmon_manifest: bool = False
    # This path can be relative, in which case it's resolved from the project_dir
    dbtmon_manifest_path: str = "target/dbtmon_manifest.json"

    # TODO: add a read from file method
    # CLI overwrites config file args by default
    # Optional argument would specify that we should not read from the config file

    # TODO: Add a classmethod that generates the default Config object as a config file in the
    # user's Home directory
