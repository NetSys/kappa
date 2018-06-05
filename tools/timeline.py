#!/usr/bin/env python3
"""
Processes Kappa logs.  See command line help for details.

Note that timeline plotting needs the `plotly` package.
"""
import argparse
from collections import defaultdict
import csv
from datetime import datetime
from enum import Enum, auto
import fileinput
import logging
import math
from operator import itemgetter
import re
import sys
from typing import Dict, NamedTuple, NewType, List, Tuple, Optional, Iterable, DefaultDict

LOG_FORMAT = re.compile(
    r"\[(?P<name>(.+-)?\d+), seqno=(?P<seqno>\d+), time=(?P<time>\d+)\]\s+(?P<msg>.+)$"
)
MESSAGE_FORMAT = re.compile(r"(?P<prefix>[^:]+):\s+(?P<content>.+)$")
TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S.%f"  # E.g., 2018/02/10 15:49:57.506261.

Pid = NewType("Pid", int)
Seqno = NewType("Seqno", int)


class LogEntryType(Enum):
    """A log entry can either be a start of an event or an end."""
    BEGIN = auto()
    END = auto()


class EventType(Enum):
    """Type of a event."""
    INVOCATION = auto()
    COMPUTE = auto()
    LOAD_CHK = auto()
    LAMBDA_RPC = auto()
    COORD_RPC = auto()
    CHKPT_S3 = auto()
    ASYNC_CHKPT_S3 = auto()
    COORD_CALL = auto()
    ASYNC_COORD_CALL = auto()


class Event(NamedTuple):
    """Represents an event on the timeline."""
    event_type: EventType
    pid: Pid
    seqno: Seqno
    start: datetime
    finish: datetime

    @property
    def duration_secs(self) -> float:
        return (self.finish - self.start).total_seconds()


event_type_map: Dict[str, EventType] = {
    # "invocation": EventType.INVOCATION,  # Invocation made by coordinator.
    "load_chk": EventType.LOAD_CHK,
    "compute": EventType.COMPUTE,
    "coordinator call": EventType.COORD_CALL,
    "async coordinator call": EventType.ASYNC_COORD_CALL,
    "rpc": EventType.LAMBDA_RPC,
    "coordinator rpc": EventType.COORD_RPC,
    "checkpoint s3": EventType.CHKPT_S3,
    "async checkpoint s3": EventType.ASYNC_CHKPT_S3,
}


def parse_message(msg: str) -> Optional[Tuple[LogEntryType, EventType]]:
    """Parses a log message; returns None if parsing fails."""
    match = re.match(MESSAGE_FORMAT, msg)
    if match is None:
        return None
    prefix = match.group("prefix")
    content = match.group("content")

    entry_type: LogEntryType
    if prefix == "begin":
        entry_type = LogEntryType.BEGIN
    elif prefix == "end":
        entry_type = LogEntryType.END
    else:
        return None

    event_type = event_type_map.get(content)
    if event_type is None:
        return None

    return entry_type, event_type


def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.
    Taken from: https://stackoverflow.com/questions/2374640/how-do-i-calculate-percentiles-with-python-numpy.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1


def print_stats(events: Iterable[Event]) -> None:
    """Prints, to standard output, aggregate duration stats for each (pid, event_type) pair in CSV form."""
    durations: DefaultDict[Tuple[Pid, EventType], List[float]] = defaultdict(list)  # Durations in seconds.
    for event in events:
        durations[(event.pid, event.event_type)].append(event.duration_secs)
    for d in durations.values():
        d.sort()

    writer = csv.writer(sys.stdout)
    writer.writerow(("pid", "type", "duration 50%", "duration 5%", "duration 95%"))
    for k in sorted(durations.keys(), key=itemgetter(0)):
        pid, event_type = k
        d = durations[k]
        writer.writerow((pid, event_type.name, percentile(d, .5), percentile(d, .05), percentile(d, .95)))


def print_all_events(events: Iterable[Event]) -> None:
    """Prints, to standard output, every event's information in CSV form."""
    event_writer = csv.writer(sys.stdout)
    event_writer.writerow(("pid", "type", "seqno", "start", "finish", "duration"))
    for event in sorted(events, key=lambda e: (e.finish.timestamp(), e.start.timestamp())):
        duration_secs = (event.finish - event.start).total_seconds()
        event_writer.writerow((event.pid, event.event_type.name, event.seqno, event.start.timestamp(),
                               event.finish.timestamp(), duration_secs))


def main() -> None:
    parser = argparse.ArgumentParser("Processes Kappa logs.")
    parser.add_argument("log_files", type=str, nargs="+", help="log files to parse")
    parser.add_argument("--print-agg", action="store_true", help="prints aggregate stats")
    parser.add_argument("--print-all", action="store_true", help="prints all events")
    parser.add_argument("--plot", action="store_true", help="plots timeline")
    args = parser.parse_args()

    events: List[Event] = []
    process_names: Dict[Pid, str] = {}
    last_start: Dict[Tuple[Pid, EventType], datetime] = {}
    last_seqno: Dict[Tuple[Pid, EventType], Seqno] = {}

    for line in fileinput.input(files=args.log_files):
        match = re.search(LOG_FORMAT, line.strip())
        if not match:
            continue

        name = match.group("name")
        pid: Pid
        try:
            pid = Pid(int(name))  # If successful, name is just the PID.
        except ValueError:
            # Otherwise, name has the form "name-pid".
            pid_str = name.rsplit("-", maxsplit=1)[-1]
            pid = Pid(int(pid_str))

            try:
                assert process_names[pid] == name  # A process had better have a unique name.
            except KeyError:
                process_names[pid] = name

        seqno = Seqno(int(match.group("seqno")))
        time_micro = int(match.group("time"))
        timestamp = datetime.fromtimestamp(time_micro / 1e6)
        message = match.group("msg").strip()

        parsed = parse_message(message)
        if parsed is None:
            continue

        log_entry_type, event_type = parsed
        event_key = (pid, event_type)
        if log_entry_type == LogEntryType.BEGIN:
            if event_key in last_start:
                logging.warning("duplicate start: %s", message)

            last_start[event_key] = timestamp
            last_seqno[event_key] = seqno
        else:
            try:
                start_ts = last_start[event_key]
                assert seqno == last_seqno[event_key]
                events.append(Event(event_type=event_type, pid=pid, seqno=seqno, start=start_ts, finish=timestamp))
                del last_start[event_key]
                del last_seqno[event_key]
            except (KeyError, AssertionError):
                logging.warning("spurious end: %s", message)

    if args.print_agg:
        print_stats(events)
    if args.print_all:
        print_all_events(events)

    if args.plot:
        import plotly
        import plotly.figure_factory as ff

        events.sort(key=lambda e: (e.pid, e.start))
        df = [dict(Task=process_names.get(e.pid, f"process_{e.pid}"),
                   Start=str(e.start), Finish=str(e.finish), EventType=e.event_type.name)
              for e in events]
        fig = ff.create_gantt(df, group_tasks=True, index_col="EventType", show_colorbar=True)
        plotly.offline.plot(fig)


if __name__ == '__main__':
    main()
