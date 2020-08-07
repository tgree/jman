# Copyright (c) 2020 by Terry Greeniaus.
from .job import Job
from .manager import Manager


current_job = None


__all__ = ['Job',
           'Manager',
           'current_job',
           ]
