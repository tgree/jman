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
    def __init__(self, u, wf):
        self.uuid      = uuid.UUID(u)
        self.wf        = wf
        self.meta      = None
        self.error_log = None

    def set_meta(self, meta):
        self.meta = meta
        self.wf.write('META: %s\n' % json.dumps(meta))
        self.wf.flush()

    def set_error_log(self, log):
        self.error_log = log
        self.wf.write('ERROR_LOG: %s\n' % json.dumps(log))
        self.wf.flush()


def main(module, function, rfd, wfd, cwd):
    # Manage file descriptors.
    rf = os.fdopen(rfd, 'r')
    wf = os.fdopen(wfd, 'w')

    # Receive our json command from the master.
    j = rf.readline()
    j = json.loads(j)
    rf.close()

    # Populate the current_job global.
    jman.current_job = CurrentJob(j['uuid'], wf)

    # Import that target module and execute the target function.
    if cwd:
        sys.path.insert(0, cwd)
    m = importlib.import_module(module)
    f = getattr(m, function)
    try:
        f(*j['args'], **j['kwargs'])
    except Exception:
        jman.current_job.set_error_log(traceback.format_exc())
        raise


def _main():
    module   = os.environ['JMAN_MODULE']
    function = os.environ['JMAN_FUNCTION']
    rfd      = int(os.environ['JMAN_RFD'])
    wfd      = int(os.environ['JMAN_WFD'])
    cwd      = os.environ.get('JMAN_CWD')

    try:
        main(module, function, rfd, wfd, cwd)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == '__main__':
    _main()
