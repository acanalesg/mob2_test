
from cdrs_util import CdrMapper
from collections import Counter

def counterSumreducer(key, values):
    c = Counter()
    for v in values:
        c = c + v
    yield key, c


def runner(job):

    ########################################
    # Step 1: Read and aggregate CDRs
    ########################################
    opts = [ ("inputformat","text"),
             ("outputformat","sequencefile"),
             ]

    o1 = job.additer(CdrMapper
        , counterSumreducer
        , combiner=counterSumreducer
        , buffersize=10000
        , opts=opts )


def starter(prog):
    pass

if __name__ == "__main__":
    from dumbo import main
    main(runner, starter)