# Copyright (c) 2020 by Terry Greeniaus.
import json
import uuid
import os

from .client import Client


class CurrentJob:
    CURRENT_JOB = None

    def __init__(self, j, wf, url):
        self.uuid      = uuid.UUID(j['uuid'])
        self.args      = j['args']
        self.kwargs    = j['kwargs']
        self.wf        = wf
        self.client    = Client(url) if url else None
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


def _load_current_job(rfd, wfd, url):
    # Manage file descriptors.
    rf = os.fdopen(rfd, 'r')
    wf = os.fdopen(wfd, 'w')

    # Receive our json command from the master.
    j = rf.readline()
    j = json.loads(j)
    rf.close()

    # Return a CurrentJob object.
    return CurrentJob(j, wf, url)


def get_current_job():
    if CurrentJob.CURRENT_JOB is not None:
        return CurrentJob.CURRENT_JOB
    if 'JMAN_RFD' not in os.environ:
        return None
    if 'JMAN_WFD' not in os.environ:
        return None
    url = os.environ.get('JMAN_SERVER')
    CurrentJob.CURRENT_JOB = _load_current_job(int(os.environ['JMAN_RFD']),
                                               int(os.environ['JMAN_WFD']),
                                               url)
    return CurrentJob.CURRENT_JOB
