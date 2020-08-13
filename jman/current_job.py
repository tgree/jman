# Copyright (c) 2020 by Terry Greeniaus.
import importlib
import traceback
import argparse
import json
import uuid
import sys
import os

import jman


class CurrentJob:
    def __init__(self, u, wfd):
        self.uuid      = uuid.UUID(u)
        self.wfd       = wfd
        self.meta      = None
        self.error_log = None

    def set_meta(self, meta):
        self.meta = meta
        self.wfd.write('META: %s\n' % json.dumps(meta))
        self.wfd.flush()

    def set_error_log(self, log):
        self.error_log = log
        self.wfd.write('ERROR_LOG: %s\n' % json.dumps(log))
        self.wfd.flush()


def main(args):
    # Manage file descriptors.
    rfd = os.fdopen(args.rfd, 'r')
    wfd = os.fdopen(args.wfd, 'w')

    # Receive our json command from the master.
    j = rfd.readline()
    j = json.loads(j)
    rfd.close()

    # Populate the current_job global.
    jman.current_job = CurrentJob(j['uuid'], wfd)

    # Import that target module and execute the target function.
    if args.cwd:
        sys.path.insert(0, args.cwd)
    m = importlib.import_module(args.module)
    f = getattr(m, args.function)
    try:
        f(*j['args'], **j['kwargs'])
    except Exception:
        jman.current_job.set_error_log(traceback.format_exc())
        raise


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--module', '-m', required=True)
    parser.add_argument('--function', '-f', required=True)
    parser.add_argument('--rfd', '-r', type=int, required=True)
    parser.add_argument('--wfd', '-w', type=int, required=True)
    parser.add_argument('--cwd', '-c')
    args = parser.parse_args()

    try:
        main(args)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == '__main__':
    _main()
