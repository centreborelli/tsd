#!/usr/bin/env python
# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@m4x.org>

import os
import sys
import multiprocessing


def show_progress(a):
    """
    Callback for the run_calls function: print nb of calls that are done.

    Args:
        a: useless argument, but since this function is used as a callback by
           apply_async, it has to take one argument.
    """
    show_progress.counter += 1
    status = 'done {:{fill}{width}} / {}'.format(show_progress.counter,
                                                           show_progress.total,
                                                           fill='',
                                                           width=len(str(show_progress.total)))
    if show_progress.counter < show_progress.total:
        status += chr(8) * len(status)
    else:
        status += '\n'
    sys.stdout.write(status)
    sys.stdout.flush()


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

    Return:
        list of outputs
    """
    results = []
    outputs = []
    show_progress.counter = 0
    show_progress.total = len(list_of_args)
    pool = multiprocessing.Pool(nb_workers)
    for x in list_of_args:
        if type(x) == tuple:
            args = x + extra_args
        else:
            args = (x,) + extra_args
        results.append(pool.apply_async(fun, args=args, callback=show_progress))

    for r in results:
        try:
            outputs.append(r.get(60))  # wait at most 1 min per call
        except multiprocessing.TimeoutError:
            print("Timeout while running %s" % str(r))
            outputs.append(None)
        except KeyboardInterrupt:
            pool.terminate()
            sys.exit(1)

    pool.close()
    pool.join()
    return outputs
