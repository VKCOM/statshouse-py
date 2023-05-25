# statshouse

StatsHouse client library for Python. Requires Python 3.7+.

## Installation

```
$ python -m pip install statshouse
```

## Usage

```python
import statshouse

statshouse.value("processing_time", {"subsystem": "foo", "protocol": "bar"}, 0.239)  # using named tags
statshouse.value("processing_time", ["foo", "bar"], 0.239)  # same, assuming `subsystem` and `protocol` are tags #1 and #2
```

`statshouse` exposes top-level functions which work with a local StatsHouse agent.
Address of the agent is configured with `--statshouse-addr` command-line flag
or `STATSHOUSE_ADDR` environment variable, and defaults to `127.0.0.1:13337`.
Additionally, `--statshouse-env` command-line flag or `STATSHOUSE_ENV` environment
variable can be used to specify the `env` tag value for all metrics (empty by default).

## Development

Install [PDM](https://pdm.fming.dev/), then:

```
$ pdm sync
$ pdm run fmt
$ pdm run lint
$ pdm run test
```
