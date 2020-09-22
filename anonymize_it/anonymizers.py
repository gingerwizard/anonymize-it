import collections

from faker import Faker
import warnings
import readers
import writers
import utils
import json
import logging

from fakers import geo_point, geo_point_key, ipv4, file_path, message, message_key, service_name, username


class AnonymizerError(Exception):
    pass


class ReaderError(Exception):
    pass


class WriterError(Exception):
    pass


class Anonymizer:
    def __init__(self, reader=None, writer=None, field_maps={}):
        """a prepackaged anonymizer class

        an anonymizer is responsible for grabbing data from the source datastore,
        masking fields specified in a config, and writing data to a destination

        can be used by

        :param reader: an instantiated reader
        :param writer: an instantiated writer
        :param field_maps: a dict like {'field.name': 'mapping_type'}
        """

        # add provider mappings here. these should map strings from the config to Faker providers
        self.provider_map = {
            "file_path": file_path,
            "ipv4": ipv4,
            "geo_point": geo_point,
            "message": message,
            "service": service_name,
            "username": username
        }

        self.provider_key_function = {
            "geo_point": geo_point_key,
            "message": message_key
        }

        self.field_maps = field_maps
        self.reader = reader
        self.writer = writer

        self.source = None
        self.dest = None
        self.masked_fields = None
        self.suppressed_fields = None
        self.reader_type = None
        self.writer_type = None

        # only parse config if it exists
        # this allows for anonymizer class instantiation via direct parameter setting
        if not self.reader:
            self.instantiate_reader()
        if not self.writer:
            self.instantiate_writer()

        # if used programmatically, an anonymizer must be instantiated with a reader and a writer
        elif not all([self.reader, self.writer]):
            raise AnonymizerError("Anonymizers must include both a reader and a writer")

    def instantiate_reader(self):
        source_params = self.source.get('params')
        if not source_params:
            raise ReaderError("source params not defined: please check config")

        reader = readers.mapping.get(self.reader_type)
        if not reader:
            raise ReaderError("No reader named {} defined.".format(self.reader_type))

        self.reader = reader(source_params, self.masked_fields, self.suppressed_fields)

    def instantiate_writer(self):
        dest_params = self.dest.get('params')
        if not dest_params:
            raise WriterError("dest params not define: please check config")

        writer = writers.mapping.get(self.writer_type)
        if not writer:
            raise WriterError("No writer named {} defined.".format(self.writer_type))

        self.writer = writer(dest_params)

    def anonymize(self, infer=False, include_rest=False):
        """this is the core method for anonymizing data

        it utilizes specific reader and writer class methods to retrieve and store data. in the process
        we define mappings of unmasked values to masked values, and anonymize fields using self.faker
        """

        # first, infer mappings based on indices and overwrite the config.
        if infer:
            self.reader.infer_providers()

        # next, create masking maps that will be used for lookups when anonymizing data
        self.field_maps = self.reader.create_mappings()

        for field, map in self.field_maps.items():
            for value, _ in map.items():
                mask_str = self.reader.masked_fields[field]
                if mask_str != 'infer':
                    mask = self.provider_map[mask_str]
                    map[value] = mask(value)

        # get generator object from reader
        total = self.reader.get_count()
        logging.info("total number of records {}...".format(total))

        data = self.reader.get_data(list(self.field_maps.keys()), self.reader.suppressed_fields, include_rest)

        # batch process the data and write out to json in chunks
        count = 0
        for batchiter in utils.batch(data, 10000):
            tmp = []
            for item in batchiter:
                bulk = {
                    "index": {
                        "_index": item.meta['index'],
                        "_type": 'doc'
                    }
                }
                tmp.append(json.dumps(bulk))
                item = utils.flatten_nest(item.to_dict())
                for field, v in item.items():
                    if self.field_maps[field]:
                        item[field] = self.field_maps[field][item[field]]
                tmp.append(json.dumps(utils.flatten_nest(item)))
            self.writer.write_data(tmp)
            count += len(tmp) / 2 # There is a bulk row for every document
            logging.info("{} % complete...".format(count/total * 100))


class LazyAnonymizer(Anonymizer):
    def __init__(self, reader=None, writer=None, field_maps={}):
        super().__init__(reader, writer, field_maps)

    # required as dictionary can be
    def __generate_field_map_key(self):
        pass

    def __delete_field_in_place(self, doc, field_path):
        if len(field_path) > 1:
            if field_path[0] in doc and  isinstance(doc[field_path[0]], collections.MutableMapping):
                deleted = self.__delete_field_in_place(doc[field_path[0]], field_path[1:])
                if deleted:
                    #check for empty key
                    if not doc[field_path[0]]:
                        del doc[field_path[0]]
                return deleted
        elif len(field_path) == 1:
            if field_path[0] in doc:
                del doc[field_path[0]]
                return True

    def __anon_field_value(self, mask_str, value):
        if not mask_str:
            return value
        field_map = self.field_maps[mask_str]
        list = False
        if mask_str in self.provider_key_function:
            # we have a means of mapping this field to a key
            mask_keys = self.provider_key_function[mask_str](value)
        else:
            if isinstance(value, collections.MutableSequence):
                mask_keys = value
                list = True
            else:
                mask_keys = [value]
        mask = self.provider_map[mask_str]
        masked_values = []
        for mask_key in mask_keys:
            if mask_key:
                if not mask_key in field_map:
                    field_map[mask_key] = mask(value)
                masked_values.append(field_map[mask_key])
            else:
                masked_values.append(mask(value))
        if not list:
            return masked_values[0]
        return masked_values

    # used when we want to keep most fields i.e. include_rest=True. Copying every field more expensive than modifying those that need to be changed
    def __anon_field_in_place(self, doc, field_path, mask_str):
        if len(field_path) > 1:
            if field_path[0] in doc and isinstance(doc[field_path[0]], collections.MutableMapping):
                self.__anon_field_in_place(doc[field_path[0]], field_path[1:], mask_str)
        elif len(field_path) == 1:
            if field_path[0] in doc:
                doc[field_path[0]] = self.__anon_field_value(mask_str, doc[field_path[0]])

    def __anon_field(self, doc, field_path, mask_str):
        new_field = {}
        if len(field_path) > 1:
            if field_path[0] in doc and isinstance(doc[field_path[0]], collections.MutableMapping):
                new_value = self.__anon_field(doc[field_path[0]], field_path[1:], mask_str)
                # field may not exist, dont create empty entries
                if not isinstance(new_value, collections.MutableMapping) or new_value:
                    new_field.update({
                        field_path[0]: new_value
                    })
        elif len(field_path):
            if field_path[0] in doc:
                return {
                    field_path[0]: self.__anon_field_value(mask_str, doc[field_path[0]])
                }
        return new_field

    def __anon_doc_include_all(self, doc, mask_fields, exclude, sep='.'):
        for field, mask_str in mask_fields.items():
            if not field in exclude:
                # update doc in place as we want all the other fields
                self.__anon_field_in_place(doc, field.split(sep), mask_str)
        for field in exclude:
            self.__delete_field_in_place(doc, field.split(sep))
        return doc

    def __anon_doc(self, doc, mask_fields, exclude, sep='.'):
        new_doc = {}
        for field, mask_str in mask_fields.items():
            if not field in exclude:
                new_doc.update(self.__anon_field(doc, field.split(sep), mask_str))
        for field in exclude:
            self.__delete_field_in_place(new_doc, field.split(sep))
        return new_doc

    def anonymize(self, infer=False, include_rest=True):
        if infer:
            self.reader.infer_providers()
        # rather than a map of values per field we create a map of values per type - this ensures fields are consistently mapped across fields in a document as well as across values
        self.field_maps = {key: {} for key in self.provider_map.keys()}
        data = self.reader.get_data(list(self.field_maps.keys()), self.reader.suppressed_fields, include_rest)
        exclude = set(self.reader.suppressed_fields)
        count = 0
        file_name = "documents-%s"
        i = 0
        for batchiter in utils.batch(data, 100000):
            tmp = []
            for item in batchiter:
                if include_rest:
                    tmp.append(json.dumps(self.__anon_doc_include_all(item, self.reader.masked_fields, exclude)))
                else:
                    tmp.append(json.dumps(self.__anon_doc(item, self.reader.masked_fields, exclude)))
            self.writer.write_data(tmp, file_name=file_name % i)
            count += len(tmp)
            logging.info(f"{count} documents complete")
            i += 1

anonymizer_mapping = {
    "default": Anonymizer,
    "lazy": LazyAnonymizer
}
