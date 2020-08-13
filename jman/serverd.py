# Copyright (c) 2020 by Terry Greeniaus.
import argparse
import sys

import jman


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bind-addr', '-b', default='0:5003')
    parser.add_argument('--max-running', '-m', default=5)
    args = parser.parse_args()

    try:
        jman.serve_forever(args.bind_addr, args.max_running)
    except KeyboardInterrupt:
        print()
        sys.exit(1)


if __name__ == '__main__':
    _main()
