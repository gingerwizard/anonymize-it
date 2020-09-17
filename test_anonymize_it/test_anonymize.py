import json

from anonymizers import LazyAnonymizer
from readers import JSONFileReader
from writers import MemoryWriter


def test_anonymize():
    reader = JSONFileReader({"filepath": "./resources/*.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point"
    }, ["user.name"])
    writer = MemoryWriter({})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)

    assert len(writer.buffer) == 4
    doc = json.loads(writer.buffer[0])
    assert doc["log"]["file"]["path"] != "/var/log/auth.log"
    assert doc["host"]["hostname"] == "vagrant-VirtualBox"
    assert doc["source"]["ip"] != "34.70.236.26"
    assert not "user" in doc