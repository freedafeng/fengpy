from itertools import izip, chain
import json


# 12/26/2013. create a generator to read a tsv file into an iterator of dictionary.
def tsv_2_dict_iterator(filename, names=None, delimiter='\t', has_header=True, strict=True, use_header_as_schema=False, chars=None):
    """
    Either names is a list, or both has_header and use_header_as_schema are true. In the latter case, we will
    use the provided schema in the data file as the dictionary's schema.
    """
    with open(filename) as fin:
        if has_header:
            line = iter(fin).next()
            if use_header_as_schema:
                names = line.strip(chars).split(delimiter)

        column_cnt = len(names)

        for line in fin:
            splited = line.strip(chars).split(delimiter)
            if strict and len(splited) != column_cnt:
                print 'Warning. wrong columns, expected column cnt %d, observed %d: %s' % \
                      (column_cnt, len(splited), line),
            else:
                yield dict(izip(names, splited))


def json_file_iterator(filename):
    """
    Each line of the file is a json string.
    """
    with open(filename) as fin:
        for line in fin:
            yield json.loads(line.strip())


def tsv_2_dict_iterator_encoding(filename, names=None, delimiter='\t', has_header=True, strict=True, use_header_as_schema=False, chars=None, encoding='utf-8'):
    """
    Either names is a list, or both has_header and use_header_as_schema are true. In the latter case, we will
    use the provided schema in the data file as the dictionary's schema.
    """
    import codecs

    with codecs.open(filename, encoding=encoding) as fin:
        if has_header:
            line = iter(fin).next()
            if use_header_as_schema:
                names = line.strip(chars).split(delimiter)

        column_cnt = len(names)

        for line in fin:
            splited = line.strip(chars).split(delimiter)
            if strict and len(splited) != column_cnt:
                print 'Warning. wrong columns, expected column cnt %d, observed %d: %s' % \
                      (column_cnt, len(splited), line),
            else:
                yield dict(izip(names, splited))


def tsv_2_dict_iterator_ignore_quotes(filename, names=None, delimiter='\t', has_header=True, strict=True, use_header_as_schema=False):
    """
    Either names is a list, or both has_header and use_header_as_schema are true. In the latter case, we will
    use the provided schema in the data file as the dictionary's schema.

    This version ignore the delimiters inside the quotes.
    """
    import csv

    with open(filename) as fin:
        if has_header:
            line = iter(fin).next()
            if use_header_as_schema:
                names = line.strip().split(delimiter)

        column_cnt = len(names)

        for line in fin:
            splited = csv.reader([line.strip()], delimiter=delimiter).next()
            if strict and len(splited) != column_cnt:
                print 'Warning. wrong columns, expected column cnt %d, observed %d: %s' % \
                      (column_cnt, len(splited), line),
            else:
                yield dict(izip(names, splited))


def iter_2_tsv(data_store, filename, columns, delimiter='\t', has_header=False):
    """
    data store is an iterator of dictionaries. Each small dictionary contains keys that are the 'columns'.
    'columns' should be a list or tuple of column names (strings). It determines the order of the columns in each line.
    """
    with open(filename, 'w') as fout:
        if has_header:
            fout.write('%s\n' % delimiter.join(columns))

        for features in data_store:
            fout.write('%s\n' % delimiter.join(str(features[column]) for column in columns))


def iter_2_tsv_shards(data_store, filename_prefix, columns, sharding_col, delimiter='\t', splits=50, mode='w'):
    """
    data store is an iterator of dictionaries. Each small dictionary contains keys that are the 'columns'.
    'columns' should be a list or tuple of column names (strings). It determines the order of the columns in each line.

    sharding_col: The key to which we are going to shard on. (get hashcode on that).
    splits: number of file that are going to be written to.
    mode: 'w', or 'a' depends on whether we want to keep the original data of the files.
    """

    fouts = [open(filename_prefix + str(split), mode) for split in xrange(splits)]

    for record in data_store:
        index = hash(record[sharding_col]) % splits
        fouts[index].write('%s\n' % delimiter.join(str(record[column]) for column in columns))

    for fout in fouts:
        fout.close()


def tsv_shards_2_dict(filename_prefix, names, splits=50, delimiter='\t', strict=True):
    """
    Read all the shards. This function returns an iterator of iterators. First level iterator is the shards,
    Second level iterator is the lines of the data files. Each line turns into a dictionary.
    """
    return (tsv_2_dict_iterator(filename_prefix + str(split), names=names, delimiter=delimiter, strict=strict, has_header=False) for split in xrange(splits))


def tsv_shards_2_dict_flattened(filename_prefix, names, splits=50, delimiter='\t', strict=True):
    """
    Read all the shards. This function returns a single iterator, which is only a flattened version of previous
    function.

    I don't think this function will be very useful, because the reason that we use sharding is to avoid the
    process of the full data set at one time. But it does have use cases.
    """
    return chain.from_iterable(tsv_shards_2_dict(filename_prefix, names, splits=splits, delimiter=delimiter, strict=strict))


def get_schema(input, delimiter='\t'):
    with open(input) as fin:
        line = iter(fin).next()
        return line.strip().split(delimiter)



