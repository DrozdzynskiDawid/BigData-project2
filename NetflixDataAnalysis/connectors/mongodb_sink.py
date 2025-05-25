from pymongo import MongoClient
import json
from NetflixDataAnalysis.tools.properties import load_properties


def write_to_mongo(json_str):
    props = load_properties("flink.properties")
    doc = json.loads(json_str)
    client = MongoClient(props.get("mongodb.url"))
    collection = client[props.get("mongodb.database")][props.get("mongodb.collection")]
    collection.insert_one(doc)
