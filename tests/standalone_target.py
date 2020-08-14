#!/usr/bin/env python3
import argparse
import time
import sys

from jman import current_job


def main(n):
    if current_job.client:
        print(current_job.client.get_jobs())
    for i in range(n):
        current_job.set_meta({'count' : i})
        time.sleep(1)


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', '-c', type=int, default=10)
    args = parser.parse_args()

    try:
        main(args.count)
    except KeyboardInterrupt:
        print()


if __name__ == '__main__':
    _main()
