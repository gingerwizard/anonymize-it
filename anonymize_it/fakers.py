import re

import faker_microservice
from faker import Faker

faker = Faker()
faker.add_provider(faker_microservice.Provider)

ip_pattern = re.compile("(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])")

def ipv4(value):
    return faker.ipv4()

def file_path(value):
    return faker.file_path()

def geo_point(value):
    location = faker.location_on_land()
    return {"country_iso_code": location[3],  "location": { "lat": location[0], "lon": location[1] }, "continent_name": location[4].split('/')[0]}

def geo_point_key(value):
    return ["%s:%s" % (value["location"]["lat"], value["location"]["lon"])]

def message_key(_):
    # dont case just process every time
    return [None]

def message(value):
    # we don't consistently replace values. Not sure it matters given this is a string field.
    return ip_pattern.sub(faker.ipv4(), value)

def service_name(value):
    return faker.microservice()