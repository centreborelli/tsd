# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@m4x.org>

from __future__ import print_function
import os
import sys
import multiprocessing
import multiprocessing.pool


def show_progress(a):
    """
    Callback for the run_calls function: print nb of calls that are done.

    Args:
        a: useless argument, but since this function is used as a callback by
           apply_async, it has to take one argument.
    """
    show_progress.counter += 1
    status = '{:{fill}{width}} / {}'.format(show_progress.counter,
                                            show_progress.total,
                                            fill='',
                                            width=len(str(show_progress.total)))
    if show_progress.counter < show_progress.total:
        status += chr(8) * len(status)
    else:
        status += '\n'
    sys.stdout.write(status)
    sys.stdout.flush()


def run_calls(fun, list_of_args, extra_args=(), pool_type='processes',
              nb_workers=multiprocessing.cpu_count(), timeout=60, verbose=True,
              initializer=None, initargs=None):
    """
    Run a function several times in parallel with different inputs.

    Args:
        fun: function to be called several times in parallel.
        list_of_args: list of (first positional) arguments passed to fun, one
            per call
        extra_args: tuple containing extra arguments to be passed to fun
            (same value for all calls)
        pool_type: either 'processes' or 'threads'
        nb_workers: number of calls run simultaneously
        timeout: number of seconds allowed per function call
        verbose: either True (show the amount of computed calls) or False
        initializer, initargs (optional): if initializer is not None then each
            worker process will call initializer(*initargs) when it starts

    Return:
        list of outputs
    """
    if pool_type == 'processes':
        pool = multiprocessing.Pool(nb_workers, initializer, initargs)
    elif pool_type == 'threads':
        pool = multiprocessing.pool.ThreadPool(nb_workers)
    else:
        print('ERROR: unknow pool_type "{}"'.format(pool_type))

    results = []
    outputs = []
    if verbose:
        show_progress.counter = 0
        show_progress.total = len(list_of_args)
    for x in list_of_args:
        if type(x) == tuple:
            args = x + extra_args
        else:
            args = (x,) + extra_args
        results.append(pool.apply_async(fun, args=args,
                                        callback=show_progress if verbose else None))

    for r in results:
        try:
            outputs.append(r.get(timeout))
        except multiprocessing.TimeoutError:
            print("Timeout while running %s" % str(r), file=sys.stderr)
            outputs.append(None)
        except KeyboardInterrupt:
            pool.terminate()
            sys.exit(1)

    pool.close()
    pool.join()
    return outputs
