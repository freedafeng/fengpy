__author__ = 'jfeng'
"""
The small tools that I am going to use very often.
"""

import collections
from itertools import izip_longest, islice, izip, chain


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


def cv_k_fold(data_size, k):
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
        yield (tuple(training), testing)


if __name__ == "__main__":
    # a = chunker(4, range(20))

    # for x in a:
    #     print x

    print list(cv_k_fold(20, 3))
    # print segmenter(3, range(20))

