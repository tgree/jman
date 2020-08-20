# Copyright (c) 2020 by Terry Greeniaus.
import threading
import json
import uuid
import os

from .client import Client


class CurrentJob:
    CURRENT_JOB = None

    def __init__(self, j, rf, wf, url):
        self.uuid        = uuid.UUID(j['uuid'])
        self.args        = j['args']
        self.kwargs      = j['kwargs']
        self.wf          = wf
        self.rf          = rf
        self.client      = Client(url) if url else None
        self.meta        = None
        self.error_log   = None
        self.istate      = None
        self.istate_cb   = None
        self.istate_lock = threading.Lock()
        self.thread      = threading.Thread(target=self._workloop, daemon=True)
        self.thread.start()

    def set_meta(self, meta):
        self.meta = meta
        self.wf.write('META: %s\n' % json.dumps(meta))
        self.wf.flush()

    def set_error_log(self, log):
        self.error_log = log
        self.wf.write('ERROR_LOG: %s\n' % json.dumps(log))
        self.wf.flush()

    def register_istate_cb(self, cb):
        with self.istate_lock:
            self.istate_cb = cb
            if self.istate is not None:
                self.istate_cb(self)

    def _workloop(self):
        while True:
            l = self.rf.readline()
            if not l:
                break

            cmd, _, data = l.partition(':')
            if cmd == 'INPUT':
                istate = json.loads(data)
                with self.istate_lock:
                    self.istate = istate
                    if self.istate_cb:
                        self.istate_cb(self)


def _load_current_job(rfd, wfd, url):
    # Manage file descriptors.
    rf = os.fdopen(rfd, 'r')
    wf = os.fdopen(wfd, 'w')

    # Receive our json command from the master.
    j = rf.readline()
    j = json.loads(j)

    # Return a CurrentJob object.
    return CurrentJob(j, rf, wf, url)


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
