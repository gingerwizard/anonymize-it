# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import logging
from contextlib import suppress

import mmap

class MmapSource:
    def __init__(self, file_name, encoding="utf-8"):
        self.file_name = file_name
        self.encoding = encoding
        self.f = None
        self.mm = None

    def open(self):
        self.f = open(self.file_name, mode="r+b")
        self.mm = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)
        # madvise is available in Python 3.8+
        with suppress(AttributeError):
            self.mm.madvise(mmap.MADV_SEQUENTIAL)

        # allow for chaining
        return self

    def seek(self, offset):
        self.mm.seek(offset)

    def read(self):
        return self.mm.read()

    def readline(self):
        return self.mm.readline()

    def close(self):
        self.mm.close()
        self.mm = None
        self.f.close()
        self.f = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __str__(self, *args, **kwargs):
        return self.file_name


class FileReader:
    def __init__(self, file_name, source_class):
        self.source_class = source_class
        self.file_name = file_name
        self.source = None
        self.current_line = 0

    def __enter__(self):
        self.open()
        return self

    def open(self):
        self.source = self.source_class(self.file_name).open()
        return self

    def close(self):
        self.source.close()
        self.source = None

    def __iter__(self):
        return self

    def __next__(self):
        return self.source.readline()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.source.close()
        return False

    def reset(self):
        self.source.seek(0)

    def __str__(self):
        return "%s[%s]" % (self.file_name, self.source)

class JSONFileSetReader:

    def __init__(self, files):
        self.readers = []
        for filename in files:
            reader = FileReader(filename, MmapSource)
            self.readers.append(reader.open())
        self._num_readers = len(self.readers)
        self._current_reader = 0

    def read(self):
        while self._current_reader != self._num_readers:
            reader = self.readers[self._current_reader]
            doc = next(reader)
            if doc:
                try:
                    doc = json.loads(doc)
                    yield doc
                except json.decoder.JSONDecodeError:
                    logging.error("Failed to decode document")
            else:
                logging.info(f"Completed file {reader.file_name}")
                reader.close()
                self._current_reader += 1
