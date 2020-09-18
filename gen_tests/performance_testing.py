import timeit

from anonymizers import LazyAnonymizer
from readers import JSONFileReader
from writers import MemoryWriter


def run_using_including_rest():
    reader = JSONFileReader({"filepath": "./nginx.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)

def run_using_excluding_rest():
    reader = JSONFileReader({"filepath": "./nginx.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=False)
    
def run_using_include_rest_message():
    reader = JSONFileReader({"filepath": "./app.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4",
        "message": "message"
    }, ["user.name"])
    writer = MemoryWriter({"keep": False})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)

if __name__ == '__main__':
    import timeit
    print("run_using_including_rest: %s" % timeit.timeit("run_using_including_rest()", setup="from __main__ import run_using_including_rest", number=100))
    print("run_using_excluding_rest: %s" % timeit.timeit("run_using_excluding_rest()",
                                                          setup="from __main__ import run_using_excluding_rest",
                                                          number=100))
    print("run_using_include_rest_message: %s" % timeit.timeit("run_using_include_rest_message()",
                                                         setup="from __main__ import run_using_include_rest_message",
                                                         number=100))