# dbtmon
A simple wrapper program for modifying dbt command output

# dbtmon

**dbtmon** is a CLI tool that wraps the output of [dbt](https://docs.getdbt.com/) commands to
improve their display for easier monitoring and readability. It's designed to integrate seamlessly 
into your workflow: just replace `dbt` with `dbtmon`.


## Features

- Acts as a drop-in replacement for `dbt <command>` with no changes to your arguments
- Keeps running jobs at the bottom of the terminal output
- Displays elapsed runtime and completed runtime in HH:MM:SS form

### dbt run
![Screenshot of the output of dbt run. Markup points out the inability to clearly see which models
are running and for how long they have been running.](/assets/dbt_screenshot.png)

### dbtmon run:

![Screenshot of the output of dbtmon run. Markup points out the running jobs grouped and displayed
in the terminal and the elapsed runtime clocks.](/assets/dbtmon_screenshot.png)


## Installation

`python -m pip install dbtmon`

View on PyPI: https://pypi.org/project/dbtmon/


## Usage

Run `dbt` commands as usual, replacing `dbt` with `dbtmon`:

```bash
dbtmon run
dbtmon test -s my_model
dbtmon build --full-refresh
```

For help or version information:

```bash
dbtmon --help
dbtmon --version
```


## Configuration

Create a configuration file at `~/.dbt/dbtmon.yaml` to customize behavior or pass config items as
command line arguments:

```yaml
# How often to refresh the display (in seconds)
polling_rate: 0.2

# Minimum wait time between updates (in seconds)
minimum_wait: 0.025
```

If no config file is present, default values are used.


## Architecture

- `dbtmon`: Main CLI entry point. Runs the `dbt` command and pipes output into a formatting process.
- `__dbtmonpipe__`: Internal entry point that reads from stdin and formats the output.
- Processing and display logic is implemented in `dbtmon/dbtmon.py`.


## License

Apache License 2.0. See the `LICENSE` file for details.
