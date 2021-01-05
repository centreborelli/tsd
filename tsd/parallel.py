# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@m4x.org>

import contextlib
import multiprocessing
import multiprocessing.pool
import sys

from tqdm.auto import tqdm


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
        raise ValueError("unknow pool_type {}".format(pool_type))

    results = []
    outputs = []

    with contextlib.ExitStack() as stack:
        if verbose:
            bar = stack.enter_context(tqdm(total=len(list_of_args)))

        for x in list_of_args:
            if type(x) == tuple:
                args = x + extra_args
            else:
                args = (x,) + extra_args
            results.append(pool.apply_async(fun, args=args,
                                            callback=lambda x: bar.update(1) if verbose else None))

        for r in results:
            try:
                outputs.append(r.get(timeout))
            except KeyboardInterrupt:
                pool.terminate()
                sys.exit(1)

    pool.close()
    pool.join()
    return outputs
