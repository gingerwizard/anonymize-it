import collections
import json

from anonymizers import LazyAnonymizer
from readers import JSONFileReader
from writers import MemoryWriter


def test_anonymize_include_rest():
    reader = JSONFileReader({"filepath": "./resources/*.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4",
        "message": "message",
        "@timestamp": None,
        "kubernetes.namespace": "service"
    }, ["user.name"])
    writer = MemoryWriter({})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=True)

    assert len(writer.buffer) == 5
    doc = json.loads(writer.buffer[0])
    assert doc["log"]["file"]["path"] != "/var/log/auth.log"
    assert doc["host"]["hostname"] == "vagrant-VirtualBox"
    assert doc["source"]["ip"] != "34.70.236.26"
    assert doc["message"] != "This has an ip of 12.12.12.44 and 12.112.13.324 which will be replaced"
    assert doc["@timestamp"] == "2020-08-16T18:09:13.000Z"
    assert not "user" in doc
    assert isinstance(doc["related"]["ip"], collections.MutableSequence)
    assert doc["related"]["user"] == ["0.397"]
    assert doc["source"]["ip"] == doc["related"]["ip"][0]
    k8_doc = json.loads(writer.buffer[1])
    assert k8_doc["kubernetes"]["namespace"] != "workplace-search-kyko-ren"
    last_doc = json.loads(writer.buffer[-1])
    assert doc["source"]["ip"] == last_doc["source"]["ip"]
    assert doc["geo"]["country_iso_code"] == last_doc["geo"]["country_iso_code"]
    assert doc["geo"]["continent_name"] == last_doc["geo"]["continent_name"]
    assert doc["geo"]["location"]["lat"] == last_doc["geo"]["location"]["lat"]
    assert doc["geo"]["location"]["lon"] == last_doc["geo"]["location"]["lon"]

def test_anonymize_limit_fields():
    reader = JSONFileReader({"filepath": "./resources/*.json"}, {
        "log.file.path": "file_path",
        "source.ip": "ipv4",
        "geo": "geo_point",
        "related.ip": "ipv4",
        "random.nest": "file_path",
        "random": "file_path",
        "another_field": "file_path",
        "user.name": "file_path",
        "@timestamp": None,
        "kubernetes.namespace": "service"
    }, ["user.name"])
    writer = MemoryWriter({})
    anon = LazyAnonymizer(reader=reader, writer=writer)
    anon.anonymize(infer=True, include_rest=False)

    assert len(writer.buffer) == 5
    doc = json.loads(writer.buffer[0])
    assert doc["log"]["file"]["path"] != "/var/log/auth.log"
    assert not "host" in doc
    assert doc["source"]["ip"] != "34.70.236.26"
    assert not "user" in doc
    assert isinstance(doc["related"]["ip"], collections.MutableSequence)
    assert not "user" in doc["related"]
    assert doc["source"]["ip"] == doc["related"]["ip"][0]
    last_doc = json.loads(writer.buffer[-1])
    assert doc["source"]["ip"] == last_doc["source"]["ip"]
    assert doc["geo"]["country_iso_code"] == last_doc["geo"]["country_iso_code"]
    assert doc["geo"]["continent_name"] == last_doc["geo"]["continent_name"]
    assert doc["geo"]["location"]["lat"] == last_doc["geo"]["location"]["lat"]
    assert doc["geo"]["location"]["lon"] == last_doc["geo"]["location"]["lon"]
    assert not "random" in doc
    assert not "another_field" in doc
    assert doc["@timestamp"] == "2020-08-16T18:09:13.000Z"
