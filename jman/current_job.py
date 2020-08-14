# Copyright (c) 2020 by Terry Greeniaus.
import json
import uuid
import os


CURRENT_JOB = None


class CurrentJob:
    def __init__(self, j, wf):
        self.uuid      = uuid.UUID(j['uuid'])
        self.args      = j['args']
        self.kwargs    = j['kwargs']
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


def _load_current_job(rfd, wfd):
    # Manage file descriptors.
    rf = os.fdopen(rfd, 'r')
    wf = os.fdopen(wfd, 'w')

    # Receive our json command from the master.
    j = rf.readline()
    j = json.loads(j)
    rf.close()

    # Return a CurrentJob object.
    return CurrentJob(j, wf)


def get_current_job():
    global CURRENT_JOB
    if CURRENT_JOB is not None:
        return CURRENT_JOB
    if 'JMAN_RFD' not in os.environ:
        return None
    if 'JMAN_WFD' not in os.environ:
        return None
    CURRENT_JOB = _load_current_job(int(os.environ['JMAN_RFD']),
                                    int(os.environ['JMAN_WFD']))
    return CURRENT_JOB
