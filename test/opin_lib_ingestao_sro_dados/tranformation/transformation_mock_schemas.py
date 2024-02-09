from pyspark.sql.types import StructType, StructField, StringType, DateType


def schema_multivalues_fields():
    return StructType([
        StructField('IDPRODUTO', StringType(), True),
        StructField('NOMEPRODUTO', StringType(), True),
        StructField('IDTIPOPRODUTO', StringType(), True),
        StructField('TIPOPRODUTO', StringType(), True),
        StructField('PROCEDENCIA', StringType(), True),
        StructField('DATA', DateType(), True),
        StructField('LIMITEMINIDADEANOS', StringType(), True),
        StructField('LIMITEMAXIDADEANOS', StringType(), True),
        StructField('CEPINICIO', StringType(), True),
        StructField('CEPFIM', StringType(), True),
        StructField('ZCHRAMO', StringType(), True)

    ])


def schema_ramos_fields():
    return StructType([
        StructField('IDRAMO', StringType(), True),
        StructField('CRAMO', StringType(), True),
        StructField('DESCRAMO', StringType(), True),
        StructField('RRAMO', StringType(), True),
        StructField('RGRP_RAMO', StringType(), True)
    ])