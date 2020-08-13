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

    def __init__(self, module, function, name, args=(), kwargs=None, cwd=None,
                 notify_meta=None, notify_complete=None, manager_notify=None):
        self.cwd             = cwd
        self.module          = module
        self.function        = function
        self.name            = name
        self.args            = args
        self.kwargs          = kwargs or {}
        self.notify_meta     = notify_meta
        self.notify_complete = [x for x in (manager_notify, notify_complete)
                                if x is not None]
        self.uuid            = uuid.uuid1()
        self.status          = Job.STATUS_PENDING
        self.thread          = None
        self.proc            = None
        self.meta            = None
        self.error_log       = None

    def spawn(self, *args, **kwargs):
        self.status = Job.STATUS_RUNNING
        self.thread = threading.Thread(target=self._workloop, daemon=True,
                                       args=args, kwargs=kwargs)
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

    def _workloop(self, verbose=False):
        rfd, child_wfd = os.pipe()
        child_rfd, wfd = os.pipe()
        rfd            = os.fdopen(rfd, 'r')
        wfd            = os.fdopen(wfd, 'w')
        cmd = ['/usr/bin/env', 'python3', '-u',
               '-m', 'jman.current_job',
               '-m', self.module,
               '-f', self.function,
               '-r', '%u' % child_rfd,
               '-w', '%u' % child_wfd,
               ]
        if self.cwd:
            cmd += ['-c', self.cwd]
        self.proc = reap.Popen(cmd, pass_fds=(child_rfd, child_wfd))
        os.close(child_rfd)
        os.close(child_wfd)

        j = json.dumps({'uuid'   : str(self.uuid),
                        'args'   : self.args,
                        'kwargs' : self.kwargs,
                        })
        wfd.write(j + '\n')
        wfd.close()

        while True:
            l = rfd.readline()
            if not l:
                break

            cmd, _, data = l.partition(':')
            if cmd == 'META':
                self.meta = json.loads(data)
                if self.notify_meta:
                    self.notify_meta(self)
                if verbose:
                    print('Got META: %s' % self.meta)
            elif cmd == 'ERROR_LOG':
                self.error_log = json.loads(data)
                if verbose:
                    print('Got ERROR_LOG: %s' % self.error_log)

        self.proc.communicate()
        if verbose:
            print('Job %s completed with return code %s.' %
                  (self.uuid, self.proc.returncode))
        self.status = Job.STATUS_COMPLETE
        for f in self.notify_complete:
            f(self)
