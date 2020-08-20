# Copyright (c) 2020 by Terry Greeniaus.
import importlib
import traceback
import sys
import os

import jman


def main(module, function, cwd):
    # Populate the current_job global.
    assert jman.current_job is not None

    # Import that target module and execute the target function.
    if cwd:
        sys.path.insert(0, cwd)
    m = importlib.import_module(module)
    f = getattr(m, function)
    try:
        f(*jman.current_job.args, **jman.current_job.kwargs)
    except Exception:
        jman.current_job.set_error_log(traceback.format_exc())
        raise


def _main():
    module   = os.environ['JMAN_MODULE']
    function = os.environ['JMAN_FUNCTION']
    cwd      = os.environ.get('JMAN_CWD')

    try:
        main(module, function, cwd)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == '__main__':
    _main()
