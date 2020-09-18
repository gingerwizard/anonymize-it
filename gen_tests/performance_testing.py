import timeit

from anonymizers import LazyAnonymizer
from readers import JSONFileReader
from writers import MemoryWriter


def run_using_modify_in_place():
    reader = JSONFileReader({"filepath": "./sample.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)


if __name__ == '__main__':
    import timeit
    print(timeit.timeit("run_using_modify_in_place()", setup="from __main__ import run_using_modify_in_place", number=100))