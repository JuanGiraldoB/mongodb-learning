from dotenv import load_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv()

MONGO_PASSWORD = os.getenv("MONGO_PWD")

MONGO_DB_URL = f"mongodb+srv://jddevgb:{MONGO_PASSWORD}@tutorial.plvwgmu.mongodb.net/?retryWrites=true&w=majority&appName=tutorial"
client = MongoClient(MONGO_DB_URL)

dbs = client.list_database_names()
test_db = client["test"]
test_collections = test_db.list_collection_names()
print(test_collections)


def insert_test_doc():
    collection = test_db['test']
    test_document = {
        "name": "Juan",
        "type": "test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)


production = client['production']
person_collection = production['person_collection']


def create_documents():
    first_names = ["Juan", "Rocket"]
    last_names = ["Diego", "Tony"]
    ages = [25, 1]

    docs = []
    for fn, ln, age in zip(first_names, last_names, ages):
        person = {
            "first_name": fn,
            "last_name": ln,
            "age": age
        }

        docs.append(person)

    person_collection.insert_many(docs)


printer = pprint.PrettyPrinter()


def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)


def find_juan():
    juan = person_collection.find_one({"first_name": "Juan"})
    printer.pprint(juan)


def count_all_people():
    count = person_collection.count_documents(filter={})
    print(f'number of people: {count}')


def get_person_by_id(person_id: int):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})

    printer.pprint(person)


# get_person_by_id("660f06c3bd4f05bf4759421b")

def get_age_range(min_age: int, max_age: int):
    query = {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}},
    ]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)


# get_age_range(1, 25)

def project_columns():
    columns = {
        "_id": 0,
        "first_name": 1,
        "last_name": 1
    }

    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)


# project_columns()

def update_person_by_id(person_id: int):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # all_updates = {
    #     "$set": {
    #         "active": True
    #     },
    #     "$inc": {
    #         "age": 1
    #     },
    #     "$rename": {
    #         "first_name": "first",
    #         "last_name": "last"
    #     }
    # }

    # person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"active": ""}})


# update_person_by_id("660f06c3bd4f05bf4759421b")


def replace_one(person_id: int):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "test",
        "last_name": "test",
        "age": 10
    }

    person_collection.replace_one({"_id": _id}, new_doc)


# replace_one("660f06c3bd4f05bf4759421b")

def delete_doc_by_id(person_id: int):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})


# delete_doc_by_id("660f06c3bd4f05bf4759421b")

# ------------------------------------------

address = {
    "_id": "660f06c3bd4f05bf4759421x",
    "street": "Sultana",
    "number": "1234",
    "city": "Manizales",
    "coutnry": "Colombia",
    "zip": "45623",
}


def add_address_embed(person_id: int, address: dict):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one(
        {"_id": _id}, {"$addToSet": {"addresses": address}})


def add_address_relationship(person_id: int, address: dict):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production['address']
    address_collection.insert_one(address)


add_address_relationship("660f136a975c5406c95c4651", address)
