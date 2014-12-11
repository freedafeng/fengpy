__author__ = 'jfeng'
"""
The small tools that I am going to use very often.
"""

import collections
from itertools import izip_longest, islice, izip, chain
import os
import sys
from datetime import datetime


def flatten(l):
    """
    Do recursive flatten from an iterable.
    If we only need to flatten a two layer iterable, we can use chain.fromiterable()
    """
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def grouper(n, iterable, padvalue=None):
    """
    grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')
    """
    return izip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


def chunker(n, iterable):
    """
    The only difference of this one with grouper is this one does not pad.
    chunker(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g',)

    This is generator that creates an iterator of tuples.
    """
    # make sure this is only an iterator. not a real list or anything.
    the_iter = iter(iterable)

    while True:
        ret = tuple(islice(the_iter, n))
        if len(ret):
            yield ret
        else:
            break


def segmenter(k, lst):
    """
    The differences of this one with chunker are,
    1. k is the number of segments we are going to split the lst into.
    2. We only accept list, no generic iterables.
    Also returns a list.
    """
    seg_size = len(lst) / k

    cnt = 0
    ret = []
    for i in range(k - 1):
        ret.append(lst[cnt: cnt + seg_size])
        cnt += seg_size

    ret.append(lst[cnt:])
    return ret


def window(seq, n=2):
    """
    to get a moving window for an iterable(iterator), use the following,
    copied from itertools.examples.
    """
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def cv_k_fold(data_size, k=5):
    """
    A generator to create a list of k fold cross validation indices for machine leanring model training.
    """
    import random

    # in case range is a generator.
    indices = list(range(data_size))

    # in place shuffle.
    random.shuffle(indices)

    # cut the list into k - pieces.
    folds = segmenter(k, indices)

    for i in range(k):
        training = chain.from_iterable(folds[j] for j in range(k) if j != i)
        testing = folds[i]
        yield list(training), testing


def sum_2_dictionaries(d1, d2):
    """
    d1 and d2 are both {key: cnt} dictionaries. we want to sum them up such as:
    d1 = {'a': 10, 'b': 1}, d2 = {'b': 3, 'c': 2}, sum_2_dictionaries(d1, d2)
    should give you {'a': 10, 'b': 4, 'c': 2}
    """

    return {x: d1.get(x, 0) + d2.get(x, 0) for x in set(d1).union(d2)}


def sum_2_2level_dicts(d1, d2):
    """
    Same as above but the dictionaries are one level deeper. Example:
    d1 = {'u1': {'a': 10, 'b': 1}, 'u2': {'b': 3, 'c': 2}}
    d2 = {'u1': {'a': 2,  'd': 3}, 'u3': {'b': 3, 'c': 2}}
    The result of sum_2_2level_dicts(d1, d2) should give
    {'u1': {'a': 12, 'b': 1, 'd': 3}, 'u2': {'b': 3, 'c': 2}, 'u3': {'b': 3, 'c': 2}}
    """
    return {x: sum_2_dictionaries(d1.get(x, {}), d2.get(x, {})) for x in set(d1).union(d2)}


def sum_2_dictionaries_generic(d1, d2, defaultfunc, addfunc):
    """
    d1 and d2 are both {key: val} dictionaries. we want to sum them up such as:
    d1 = {'a': [1, 2], 'b': [3, 0], 'd': [0, 4]} d2 = {'b': [3, 3], 'c': [2, 1]},
    sum_2_dictionaries_generic(d1, d2, defaultfunc=lambda: [0, 0], addfunc=lambda x, y: [x[0] + y[0], x[1] + y[1]])
    should give you {'a': [1, 2], 'b': [6, 3], 'c': [2, 1], 'd': [0, 4]}

    The reason that we use defaultfunc instead of default value is that a mutable default value could and will always
    cause problems.
    """

    return {x: addfunc(d1.get(x, defaultfunc()), d2.get(x, defaultfunc())) for x in set(d1).union(d2)}


def sum_2_2level_dicts_generic(d1, d2, defaultfunc, addfunc):
    """
    Same as above but the dictionaries are one level deeper. Example:
    d1 = {'u1': {'a': [1, 2], 'b': [3, 0], 'd': [0, 4]}, 'u2': {'b': [3, 3], 'c': [2, 1]}}
    d2 = {'u1': {'a': [1, 2], 'd': [0, 4]}, 'u2': {'b': [3, 3], 'c': [2, 1]}, 'u3': {'a': [3, 2], 'f': [1, 0]}}
    The result of sum_2_2level_dicts_generic(d1, d2, defaultfunc=lambda: [0, 0],
    addfunc=lambda x, y: [x[0] + y[0], x[1] + y[1]]) should give you
    {'u1': {'a': [2, 4], 'b': [3, 0], 'd': [0, 8]}, 'u2': {'c': [4, 2], 'b': [6, 6]}, 'u3': {'a': [3, 2], 'f': [1, 0]}}
    """
    return {x: sum_2_dictionaries_generic(d1.get(x, {}), d2.get(x, {}), defaultfunc, addfunc) for x in set(d1).union(d2)}


def disp_tm_msg(msg):
    print 'time [%s]: %s' % (datetime.now(), msg)
    sys.stdout.flush()


def disp_tm_msg_parallel(msg):
    """
    This is likely to be used in a long run job with multiple processes, when os.getpid() cost little compared to
    what the job is doing.
    :param msg: the message we want to print.
    :return: None
    """
    print 'process [%s] time [%s]: %s' % (os.getpid(), datetime.now(), msg)
    sys.stdout.flush()


if __name__ == "__main__":
    # a = chunker(4, range(20))

    # for x in a:
    #     print x

    print list(cv_k_fold(20, 3))
    # print segmenter(3, range(20))

    disp_tm_msg_parallel("Hello, world!")


