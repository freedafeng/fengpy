__author__ = 'jfeng'
"""
The small tools that I am going to use very often.
"""

import collections
from itertools import izip_longest, islice, izip


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


if __name__ == "__main__":
    a = chunker(4, range(20))

    for x in a:
        print x
