# COMMAND ----------
# DBTITLE 1,Imports
from pymongo import MongoClient

# COMMAND ----------
# DBTITLE 1,User Variables
client = "mongo_uri" # Alterar
db = client["mongo_database"] # Alterar
collection = db["mongo_collection"] # Alterar

# Definir o campo que será usado como chave para deleção, alterando o rowId conforme necessidade. 
# Recomenda-se um campo da collection que seja chave única.
field = "rowId"
fields_key = {"_id": 0, field: 1}

# Tamanho da amostra que será deletada por vez. 
# Esse valor deverá ser ajustado conforme necessidade pois se a collection possui documentos muito grandes, a deleção deverá ser por blocos menores para evitar timeout.
size = 1000

# COMMAND ----------
# DBTITLE 1,Delete batch logic
result = []

for value in collection.find({}, fields_key):
    result.append(value["rowId"])

while result:
    sample = result[:size]
    collection.delete_many({field: {"$in": sample}})
    [result.remove(value) for value in sample if value in result]