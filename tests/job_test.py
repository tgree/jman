#!/usr/bin/env python3
import time
import sys

from jman import Job, current_job


def task(n):
    for i in range(n):
        current_job.set_meta({'count' : i})
        time.sleep(1)


def notify_meta(j):
    print('Job %s: META updated to %s' % (j.uuid, j.meta))


def notify_complete(j):
    print('Job %s: completed with status %u' % (j.uuid, j.proc.returncode))


if __name__ == '__main__':
    j = Job.from_mod_func('tests.job_test', 'task', 'TEST-TASK', args=(10,),
                          notify_meta=notify_meta,
                          notify_complete=notify_complete)
    j.spawn()
    j.join()
