#!/usr/bin/env python
# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@m4x.org>

import os
import sys
import pebble


def show_progress():
    """
    Callback for the run_calls function: print nb of calls that are done.
    """
    show_progress.counter += 1
    status = 'done {:{fill}{width}} / {}'.format(show_progress.counter,
                                                 show_progress.total, fill='',
                                                 width=len(str(show_progress.total)))
    if show_progress.counter < show_progress.total:
        status += chr(8) * len(status)
    else:
        status += '\n'
    sys.stdout.write(status)
    sys.stdout.flush()


def task_done(future):
    """
    """
    try:
        result = future.result()  # blocks until results are ready
        show_progress()
    except Exception as error:
        print("Function raised %s" % error)
        print(error.traceback)  # traceback of the function
    except TimeoutError as error:
        print("Function took longer than %d seconds" % error.args[1])


def run_calls(fun, list_of_args, nb_workers, *extra_args):
    """
    Run a function several times in parallel with different inputs.

    Args:
        fun: function to be called several times in parallel.
        list_of_args: list of (first positional) arguments passed to fun, one
            per call
        nb_workers: number of calls run simultaneously
        extra_args (optional): tuple containing extra arguments to be passed to
            fun (same value for all calls)
    """
    show_progress.counter = 0
    show_progress.total = len(list_of_args)
    pool = pebble.ProcessPool(nb_workers)
    for x in list_of_args:
        if type(x) == tuple:
            args = x + extra_args
        else:
            args = (x,) + extra_args
        r = pool.schedule(fun, args=args, timeout=120)  # wait at most 2 min per call
        r.add_done_callback(task_done)
