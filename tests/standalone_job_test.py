#!/usr/bin/env python3
import time
import os

from jman import Job, current_job


DIR = os.path.dirname(os.path.realpath(__file__))


def notify_meta(j):
    print('Job %s: META updated to %s' % (j.uuid, j.meta))


def notify_complete(j):
    print('Job %s: completed with status %u' % (j.uuid, j.proc.returncode))


if __name__ == '__main__':
    j = Job(None, None, 'TEST-TASK',
            cmd=['/usr/bin/env', 'python3',
                 os.path.join(DIR, 'standalone_target.py'),
                 '-c', '10'],
            notify_meta=notify_meta, notify_complete=notify_complete)
    j.spawn()
    j.join()
