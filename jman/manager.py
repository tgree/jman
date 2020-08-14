# Copyright (c) 2020 by Terry Greeniaus.
import threading

from .job import Job


class Manager:
    def __init__(self, max_running=2):
        self.max_running  = max_running
        self.jobs_lock    = threading.Lock()
        self.jobs         = {}
        self.jobs_by_name = {}
        self.running_jobs = set()
        self.pending_jobs = []

    def __getitem__(self, uuid):
        return self.jobs[uuid]

    def get_job_by_name(self, name):
        return self.jobs_by_name[name]

    def _spawn(self, factory, *args, **kwargs):
        with self.jobs_lock:
            j = factory(*args, manager_notify=self.notify_complete, **kwargs)
            if j.name in self.jobs_by_name:
                raise Exception('Duplicate name: %s' % j.name)
            self.jobs[j.uuid] = j
            if j.name:
                self.jobs_by_name[j.name] = j
            if len(self.running_jobs) < self.max_running:
                self.running_jobs.add(j)
                j.spawn()
            else:
                self.pending_jobs.append(j)
            return j

    def spawn_mod_func(self, *args, **kwargs):
        return self._spawn(Job.from_mod_func, *args, **kwargs)

    def spawn_cmd(self, *args, **kwargs):
        return self._spawn(Job.from_cmd, *args, **kwargs)

    def notify_complete(self, j):
        with self.jobs_lock:
            self.running_jobs.remove(j)
            if self.pending_jobs:
                j = self.pending_jobs.pop(0)
                self.running_jobs.add(j)
                j.spawn()

    def join(self, j, timeout=None):
        j.join(timeout=timeout)
        if j.status == j.STATUS_COMPLETE:
            with self.jobs_lock:
                del self.jobs[j.uuid]
                if j.name:
                    del self.jobs_by_name[j.name]

    def join_all(self):
        while self.jobs:
            self.join(next(iter(self.jobs.values())))
