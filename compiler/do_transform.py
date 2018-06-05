#!/usr/bin/env python3
"""Takes Python source code from standard input and prints CPS-transformed code to standard output."""
import argparse
import ast
import sys
# base_path = os.path.dirname(os.path.realpath(__file__))
# sys.path.append(os.path.realpath(os.path.join(base_path, '3rdparty', 'astor')))

import astor

from transform import transform


def main():
    parser = argparse.ArgumentParser(description="Kappa compiler")
    parser.add_argument("--auto-pause", action="store_true", help="Automatically insert pause points.")
    args = parser.parse_args()

    source = sys.stdin.read()
    mod = ast.parse(source)
    transformed = transform(mod, auto_pause=args.auto_pause)
    print(astor.to_source(transformed))  # Print out resulting AST as code.


if __name__ == '__main__':
    main()
