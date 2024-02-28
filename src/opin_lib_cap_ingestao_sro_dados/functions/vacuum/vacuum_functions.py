from datetime import datetime
import time
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from opin_lib_cap_ingestao_sro_dados import storage_functions as sf

vacuum_hours = 168


def vacuum_log_schema():
    return StructType([\
        StructField('tabela', StringType(), True),\
        StructField('qt_anterior', StringType(), True),\
        StructField('gb_anterior', StringType(), True),\
        StructField('mb_anterior', StringType(), True),\
        StructField('qt_vacuum', StringType(), True),\
        StructField('gb_vacuum', StringType(), True),\
        StructField('mb_vacuum', StringType(), True),\
        StructField('obs', StringType(), True),\
        StructField('tempo_execucao_min', FloatType(), True),\
        StructField('dt_execucao', StringType(), True)
    ])


def sum_table_size(dbutils, path):
    files = sf.get_files(dbutils, path)
    size = sum([item.size for item in files])
    return size


def get_table_size(spark, dbutils, path):
    size_bytes = sum_table_size(dbutils, path)
    quant = spark.read.format('delta').load(path).count()
    size_mb = (size_bytes / (1024 ** 2))
    size_gb = (size_bytes / (1024 ** 3))

    return {'quant': str(quant),
            'size_gb': "%.6f" % size_gb,
            'size_mb': "%.6f" % size_mb}


def vacuum_delta(dbutils, spark, path_tabela: str, path_log: str):
    dt_execucao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    if not sf.is_empty(dbutils, path_tabela):
        l_anterior = get_table_size(spark, dbutils, path_tabela)

        start_time = time.time()
        spark.sql(f"VACUUM delta.`{path_tabela}` RETAIN {vacuum_hours} HOURS")
        exec_time = (time.time() - start_time) / 60

        l_vacuum = get_table_size(spark, dbutils, path_tabela)

        log = [{'tabela': path_tabela,
                'qt_anterior': l_anterior['quant'],
                'gb_anterior': l_anterior['size_gb'],
                'mb_anterior': l_anterior['size_mb'],
                'qt_vacuum': l_vacuum['quant'],
                'gb_vacuum': l_vacuum['size_gb'],
                'mb_vacuum': l_vacuum['size_mb'],
                'tempo_execucao_min': float("%f" % exec_time),
                'dt_execucao': dt_execucao}]
    else:
        log = [{'tabela': path_tabela,
                'obs': 'Tabela não encontrada',
                'dt_execucao': dt_execucao}]

    df = spark.createDataFrame(log, vacuum_log_schema())
    sf.write_delta_file(df, path_log, 'append')
