__author__ = 'acg'

from dumbo.lib import MultiMapper, JoinReducer
from dumbo.decor import primary, secondary

sep="|"

def cellLkpMapper(key, value):
    toks = value.split(sep)
    # cell, bts
    yield toks[0], toks[1]

def vectorsMapper(key, value):
    # cell, (user, vectors)
    yield key[0], (key[1], value)


class joinReducer(JoinReducer):
    def primary(self, key, values):
        self.bts = values.next()

    def secondary(self, key, values):
        if hasattr(self, 'bts'):
            for v in values:
                yield (self.bts, v[0]), v[1]



def runner(job):
    multimap = MultiMapper()
    multimap.add("cells", primary(cellLkpMapper))
    multimap.add("vector", secondary(vectorsMapper))
    o1 = job.additer( multimap
        , joinReducer )


if __name__ == "__main__":
    from dumbo import main
    main(runner)