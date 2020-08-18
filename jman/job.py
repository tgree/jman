# Copyright (c) 2020 by Terry Greeniaus.
import threading
import json
import uuid
import os

import reap


class Job:
    STATUS_PENDING  = 0
    STATUS_RUNNING  = 1
    STATUS_COMPLETE = 2

    def __init__(self, name, module=None, function=None, args=(), kwargs=None,
                 cmd=None, cwd=None, notify_meta=None, notify_complete=None,
                 manager_notify=None):
        self.name            = name
        self.module          = module
        self.function        = function
        self.args            = args
        self.kwargs          = kwargs or {}
        self.cmd             = cmd or ['/usr/bin/env', 'python3',
                                       '-m', 'jman.mod_func_loader']
        self.cwd             = cwd
        self.notify_meta     = notify_meta
        self.notify_complete = [x for x in (manager_notify, notify_complete)
                                if x is not None]
        self.uuid            = uuid.uuid1()
        self.status          = Job.STATUS_PENDING
        self.wfd             = None
        self.wfd_lock        = threading.Lock()
        self.thread          = None
        self.proc            = None
        self.meta            = None
        self.istate          = None
        self.error_log       = None

    @staticmethod
    def from_mod_func(module, function, name=None, **kwargs):
        assert 'cmd' not in kwargs
        return Job(name, module=module, function=function, **kwargs)

    @staticmethod
    def from_cmd(cmd, name=None, **kwargs):
        assert 'module' not in kwargs
        assert 'function' not in kwargs
        return Job(name, cmd=cmd, **kwargs)

    def spawn(self):
        self.status = Job.STATUS_RUNNING
        self.thread = threading.Thread(target=self._workloop, daemon=True)
        self.thread.start()

    def join(self, timeout=None):
        self.thread.join(timeout)

    def get_status_str(self):
        if self.status == Job.STATUS_PENDING:
            return 'PENDING'
        if self.status == Job.STATUS_RUNNING:
            return 'RUNNING'
        if self.status == Job.STATUS_COMPLETE:
            return 'COMPLETE'
        return '???'

    def _write_istate(self):
        # Assumes wfd_lock is held.
        if self.wfd is not None:
            self.wfd.write('INPUT: %s\n' % json.dumps(self.istate))
            self.wfd.flush()

    def set_istate(self, istate):
        with self.wfd_lock:
            self.istate = istate
            self._write_istate()

    def _workloop(self):
        rfd, child_wfd       = os.pipe()
        child_rfd, wfd       = os.pipe()
        rfd                  = os.fdopen(rfd, 'r')
        wfd                  = os.fdopen(wfd, 'w')
        env                  = os.environ.copy()
        if self.module:
            env['JMAN_MODULE'] = self.module
        if self.function:
            env['JMAN_FUNCTION'] = self.function
        if self.cwd:
            env['JMAN_CWD'] = self.cwd
        env['JMAN_RFD']      = str(child_rfd)
        env['JMAN_WFD']      = str(child_wfd)
        with self.wfd_lock:
            self.wfd  = wfd
            self.proc = reap.Popen(self.cmd, pass_fds=(child_rfd, child_wfd),
                                   env=env, cwd=self.cwd)
            os.close(child_rfd)
            os.close(child_wfd)

            j = json.dumps({'uuid'   : str(self.uuid),
                            'args'   : self.args,
                            'kwargs' : self.kwargs,
                            })
            wfd.write(j + '\n')
            wfd.flush()

            if self.istate is not None:
                self._write_istate()

        while True:
            l = rfd.readline()
            if not l:
                break

            cmd, _, data = l.partition(':')
            if cmd == 'META':
                self.meta = json.loads(data)
                if self.notify_meta:
                    self.notify_meta(self)
            elif cmd == 'ERROR_LOG':
                self.error_log = json.loads(data)

        self.proc.communicate()
        self.status = Job.STATUS_COMPLETE
        for f in self.notify_complete:
            f(self)
