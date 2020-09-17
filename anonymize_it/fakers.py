from faker import Faker
faker = Faker()

def ipv4():
    return faker.ipv4()

def file_path():
    return faker.file_path()

def geo_point():
    location = faker.location_on_land()
    return {"country_iso_code": location[3],  "location": { "lat": location[0], "lon": location[1] }, "continent_name": location[4].split('/')[0]}

def geo_point_key(doc):
    return "%s:%s" % (doc["location"]["lat"], doc["location"]["lon"])