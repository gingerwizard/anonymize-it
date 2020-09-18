import timeit

from anonymizers import LazyAnonymizer
from readers import JSONFileReader
from writers import MemoryWriter


def run_using_including_reset():
    reader = JSONFileReader({"filepath": "./sample.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)

def run_using_excluding_reset():
    reader = JSONFileReader({"filepath": "./sample.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=False)

if __name__ == '__main__':
    import timeit
    print("run_using_including_reset: %s" % timeit.timeit("run_using_including_reset()", setup="from __main__ import run_using_including_reset", number=100))
    print("run_using_excluding_reset: %s" % timeit.timeit("run_using_excluding_reset()",
                                                          setup="from __main__ import run_using_excluding_reset",
                                                          number=100))