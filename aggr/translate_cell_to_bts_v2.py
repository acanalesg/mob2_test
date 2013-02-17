__author__ = 'acg'

"""
   The original version of this file required
   JoinReducer ==> hadoop BinaryPartitioner

   This is a dirtier version, that does not
   require it

"""

from dumbo.lib import MultiMapper
from dumbo import identitymapper
from collections import Counter


sep="|"

def cellLkpMapper(key, value):
    toks = value.split(sep)
    # cell, (0, bts)
    yield toks[0], (0, toks[1])

def vectorsMapper(key, value):
    # cell, (1, user, vectors)
    yield key[0], (1, key[1], value)

def mergeReducer(key, values):
    # values come ordered by value ==> cellLkp first
    # Take bts from lkp, counter and user from vector
    bts = "not found"
    for v in values:
        if v[0] == 0:
            bts = v[1]
        else:
            yield (bts, v[1]), v[2]

def counterSumreducer(key, values):
    c = Counter()
    for v in values:
        c = c + v
    yield key, c

def runner(job):
    multimap = MultiMapper()
    multimap.add("cells", cellLkpMapper)
    multimap.add("vector", vectorsMapper)
    o1 = job.additer( multimap
        , mergeReducer )

    o2 = job.additer( identitymapper
        , counterSumreducer)

if __name__ == "__main__":
    from dumbo import main
    main(runner)