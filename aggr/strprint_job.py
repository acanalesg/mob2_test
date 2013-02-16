
from collections import Counter

def stringMapper(key, value):
     yield str(key), str(value)

def runner(job):

    opts = [ ("inputformat","sequencefile"),
             ("outputformat","text"),
             ("numreducetasks","0")
           ]

    o = job.additer(stringMapper
        , opts=opts )


if __name__ == "__main__":
    from dumbo import main
    main(runner)