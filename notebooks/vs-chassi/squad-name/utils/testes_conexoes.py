# COMMAND ----------
# DBTITLE 1,Install cluster libs
# MAGIC %pip install /dbfs/FileStore/jars/commons/jproperties-2.1.1-py2.py3-none-any.whl
# MAGIC %pip install /dbfs/FileStore/jars/commons/pymongo-4.0.1-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
# MAGIC %pip install /dbfs/FileStore/jars/vs-chassi/opin-lib-chassis-dados/opin_lib_chassis_dados-1.0.0-py3-none-any.whl

# COMMAND ----------
# DBTITLE 1,Import
from opin_lib_testes_conexoes.config.context import Context, Environment
from opin_lib_testes_conexoes.runners.storage import test_storage
from opin_lib_testes_conexoes.runners.kafka import test_kafka
from opin_lib_testes_conexoes.runners.cosmos import test_cosmos


# COMMAND ----------
# DBTITLE 1,Run process
ENV = Environment()
CONTEXT = Context(spark, ENV, dbutils)

def main():
    test_storage(CONTEXT)
    test_kafka(CONTEXT)
    test_cosmos(CONTEXT)


if __name__ == '__main__':
    main()
    