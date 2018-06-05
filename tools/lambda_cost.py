#!/usr/bin/env python3
"""Calculates AWS Lambda invocation cost ($) from log, assuming 3008-MB lambda memory."""
import argparse
from pathlib import Path
import re
import sys

pattern = r"REPORT RequestId: .+\tDuration: .+ ms\tBilled Duration: (\d+) ms"


def main():
    parser = argparse.ArgumentParser(description="Calculates lambda cost.")
    parser.add_argument("log_dir", type=str, help="Log directory for a run.")
    args = parser.parse_args()

    num_invocations = 0
    total_ms = 0

    log_dir = Path(args.log_dir)
    with (log_dir / "handlers.log").open("r") as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                num_invocations += 1
                total_ms += int(match.group(1))

    price = 0.000004897*total_ms/100 + 0.0000002*num_invocations
    print(price)


if __name__ == "__main__":
    main()

