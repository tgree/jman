#!/usr/bin/env python3
import time
import sys

from jman import Manager, current_job


def task(n):
    for i in range(n):
        current_job.set_meta({'count' : i})
        time.sleep(0.1)


def notify_meta(j):
    print('Job %s: META updated to %s' % (j.uuid, j.meta))


def notify_complete(j):
    print('Job %s: completed with status %u' % (j.uuid, j.proc.returncode))


if __name__ == '__main__':
    m = Manager()
    jobs = [m.spawn('tests.manager_test', 'task', 'TASK-0', args=(10,),
                    notify_meta=notify_meta, notify_complete=notify_complete),
            m.spawn('tests.manager_test', 'task', 'TASK-1', args=(3,),
                    notify_meta=notify_meta, notify_complete=notify_complete),
            m.spawn('tests.manager_test', 'task', 'TASK-2', args=(8,),
                    notify_meta=notify_meta, notify_complete=notify_complete),
            m.spawn('tests.manager_test', 'task', 'TASK-3', args=(5,),
                    notify_meta=notify_meta, notify_complete=notify_complete),
            ]
    m.join_all()
    for j in jobs:
        print('Job %s (%s) result: %s' % (j.uuid, j.name, j.proc.returncode))
    assert not m.jobs
