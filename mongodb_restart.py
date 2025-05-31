from pymongo import MongoClient
from NetflixDataAnalysis.tools.properties import load_properties

props = load_properties("NetflixDataAnalysis/flink.properties")
client = MongoClient(props.get("mongodb.url"))
db = client[props.get("mongodb.database")]
collection_name = props.get("mongodb.collection")

db.drop_collection(collection_name)
print(f"Kolekcja '{collection_name}' została usunięta.")
