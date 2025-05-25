import json
from pymongo import MongoClient
from NetflixDataAnalysis.tools.properties import load_properties

props = load_properties("NetflixDataAnalysis/flink.properties")
client = MongoClient(props.get("mongodb.url"))
collection = client[props.get("mongodb.database")][props.get("mongodb.collection")]
docs = list(collection.find())
docs_json = [json.dumps(doc, default=str) for doc in docs]

for line in docs_json:
    print(line)
