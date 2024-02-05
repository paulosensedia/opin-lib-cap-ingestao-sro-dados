from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql import types as T
from opin_lib_cap_ingestao_sro_dados import storage_functions as sf

var_format_date = "yyyy-MM-dd HH:mm:ss"


def valida_volumetria(dbutils, spark, table, path_bronze, path_validacao):
    # Count Bronze
    df_bronze_last_id = sf.read_delta_file(dbutils, spark, path_bronze)
    max_dt = df_bronze_last_id.agg(f.max('DT_INGESTAO_BRONZE')).collect()[0][0]
    df_bronze_last_id = df_bronze_last_id.filter(f.col('DT_INGESTAO_BRONZE') == max_dt)
    rows_bronze_last_id = int(df_bronze_last_id.count())

    # Count Hist Bronze
    delta_table = DeltaTable.forPath(spark, path_bronze)
    hist = delta_table.history()
    hist.registerTempTable('METRICS_OUTPUT_ROWS')

    df = spark.sql(f'''
                        SELECT D.version, D.operationMetrics 
                        FROM METRICS_OUTPUT_ROWS D 
                        WHERE D.version = (SELECT MAX(M.version) FROM METRICS_OUTPUT_ROWS M)
                      ''')
    try:
        rows_inserted = int(df[['operationMetrics.numTargetRowsInserted']].collect()[0][0])
        rows_updated = int(df[['operationMetrics.numTargetRowsUpdated']].collect()[0][0])
        rows_affected = rows_inserted + rows_updated
    except Exception:
        rows_affected = int(df[['operationMetrics.numOutputRows']].collect()[0][0])

    if rows_bronze_last_id == rows_affected:
        status_validacao = 'OK'
    else:
        status_validacao = 'NOK'

    df_validacao = [(table, rows_bronze_last_id, rows_affected, status_validacao)]

    schema = T.StructType([
        T.StructField("RTBELA", T.StringType(), True),
        T.StructField("QVOLUM_ORIGE", T.IntegerType(), True),
        T.StructField("QVOLUM_DSTNO", T.IntegerType(), True),
        T.StructField("RSTTUS", T.StringType(), True),
    ])

    df_validacao = spark.createDataFrame(data=df_validacao, schema=schema)
    df_validacao = df_validacao.withColumn('DHCARGA',
                                           f.date_format(f.to_timestamp(f.current_timestamp()), var_format_date))

    sf.write_delta_file(df_validacao, path_validacao, 'append')


def valida_obrigatoriedade(dbutils, spark, chaves, path_parametros, path_bronze, path_validacao):
    # Lê arquivo de parâmetros
    df_validacao = sf.read_csv_file(dbutils, spark, '|', path_parametros, True)

    if (df_validacao is not None) and (df_validacao.count() > 0):
        campos_obrig = df_validacao.where('obrigatorio is true').rdd.map(lambda x: x[0]).collect()

        # Lê a última ingestão da Bronze
        spark.read.format("delta").load(path_bronze).registerTempTable('TMP_BRONZE')
        df_invalidos = spark.sql(f'''
                        SELECT * FROM TMP_BRONZE B 
                        WHERE B.DT_INGESTAO_BRONZE = (SELECT MAX(M.DT_INGESTAO_BRONZE) FROM TMP_BRONZE M)
                      ''')

        new_column = 'RCPO_OBRIG_NAO_PREEN'
        chaves.append(new_column)

        df_invalidos = df_invalidos \
            .withColumn(new_column,
                        f.array(
                            *[f.when((f.col(c).isNull() | (f.col(c) == f.lit(""))), f.lit(c)) for c in campos_obrig])
                        ) \
            .withColumn(new_column, f.expr("filter(" + new_column + ", c -> c IS NOT NULL)")) \
            .withColumn(new_column, f.concat_ws(",", f.col(new_column)))

        df_invalidos = df_invalidos[chaves].where(f.col(new_column) != f.lit(""))
        df_invalidos = df_invalidos.withColumn('DHCARGA',
                                               f.date_format(f.to_timestamp(f.current_timestamp()),
                                                             var_format_date))

        sf.write_delta_file(df_invalidos, path_validacao, 'append')


def valida_tipagem(dbutils, spark, table, path_parametros, path_bronze, path_validacao):
    # Lê arquivo de parâmetros
    df_validacao = sf.read_csv_file(dbutils, spark, '|', path_parametros, True)

    if (df_validacao is not None) and (df_validacao.count() > 0):
        # Lê a última ingestão da Bronze
        spark.read.format("delta").load(path_bronze).registerTempTable('TMP_BRONZE')
        df_bronze = spark.sql(f'''
                            SELECT * FROM TMP_BRONZE B 
                            WHERE B.DT_INGESTAO_BRONZE = (SELECT MAX(M.DT_INGESTAO_BRONZE) FROM TMP_BRONZE M)
                          ''')

        # Monta DF com colunas e tipos
        col = ["campo", "tipo"]
        df_bronze = spark.createDataFrame(data=df_bronze.dtypes, schema=col)

        df_bronze = df_bronze.withColumn('tipo', f.regexp_replace(df_bronze["tipo"], 'bigint', 'int'))
        df_bronze = df_bronze.withColumn('tipo', f.when(f.col('tipo').contains('decimal'),
                                                        f.lit('decimal'))
                                         .otherwise(f.col('tipo')))

        # Monta estrutura de Registros Inválidos
        schema = T.StructType([
            T.StructField("RTBELA", T.StringType(), True),
            T.StructField("RCPO", T.StringType(), True),
            T.StructField("RTPO_ESPRD", T.StringType(), True),
            T.StructField("RTPO_RECBD", T.StringType(), True)
        ])

        # Revisa o tipo de todos os campos salvando na tabela os que estão inválidos
        df_invalidos = []

        for t in df_bronze.collect():
            df = df_validacao.where(df_validacao.campo == t.campo)
            if df.count() == 1:
                tipo = df[['tipo']].collect()[0][0]
                if tipo != t.tipo:
                    df_invalidos += [(table, t.campo, tipo, t.tipo)]

        df_invalidos = spark.createDataFrame(data=df_invalidos, schema=schema)
        df_invalidos = df_invalidos.withColumn('DHCARGA',
                                               f.date_format(f.to_timestamp(f.current_timestamp()),
                                                             "yyyy-MM-dd HH:mm:ss"))

        sf.write_delta_file(df_invalidos, path_validacao, 'append')


def log_error(spark, table, error_message, path_log):
    log_error = [{
        'tabela': table,
        'erro': error_message}]

    schema = T.StructType([ \
            T.StructField('tabela', T.StringType(), True), \
            T.StructField('erro', T.StringType(), True), \
        ])

    df_log = spark.createDataFrame(log_error, schema)
    df_log = df_log.withColumn('dt_execucao', f.date_format(f.to_timestamp(f.current_timestamp()), var_format_date))
    sf.write_delta_file(df_log, path_log, 'append')
