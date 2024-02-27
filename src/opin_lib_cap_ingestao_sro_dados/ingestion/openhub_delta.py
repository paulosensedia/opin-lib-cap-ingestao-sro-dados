from delta import DeltaTable
from opin_lib_cap_ingestao_sro_dados import common_functions as cf
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados import storage_functions as sf
from opin_lib_cap_ingestao_sro_dados.transformation.transformation_functions import format_date_time, rename_columns, \
    add_ramo_susep
import pyspark.sql.functions as f
from pyspark.sql import Window


def get_tables_name_delta(dbutils, spark, path):
    tables = sf.read_csv_file(dbutils, spark, '|', path, True)
    tables = tables.withColumn('path_source', f.concat(f.col('base'), f.lit('/'), f.col('table')))
    tables = tables.withColumn('path_sink',
                               f.concat(f.col('base'), f.lit('/'), f.regexp_replace('table', 'ZOHOPCP', 'ZDAOPCP')))

    return tables


def merge_delta(full, delta, chaves_list):
    condition = ''
    max_list = len(chaves_list) - 1
    count = 0

    for i in chaves_list:
        if count < max_list:
            condition += 'f.' + i + ' = d.' + i + ' and '
            count += 1
        else:
            condition += 'f.' + i + ' = d.' + i

    full.alias('f').merge(delta.alias('d'),
                          condition=condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    return full


def max_delta(df, chaves_list):
    w = Window().partitionBy(chaves_list).orderBy(chaves_list)
    df = df.withColumn('max', f.max('ZCHREQUID').over(w)).where(f.col('ZCHREQUID') == f.col('max')).drop(f.col('max'))
    df = df.withColumn('max', f.max('RECORD').over(w)).where(f.col('RECORD') == f.col('max')).drop(f.col('max'))

    return df


def ingestion(dbutils, spark, context, ramos_path, tables_path, input_path, output_path):
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    ramos = sf.read_delta_file(dbutils, spark, ramos_path)
    tables = get_tables_name_delta(dbutils, spark, tables_path + '.csv')

    for item in tables.collect():
        delta = sf.read_csv_file(dbutils, spark, '|', input_path + '/' + item.path_source + '.csv', True)

        if (delta is not None) and (delta.count() > 0):
            chaves_list = list(item.chaves.split(", "))

            # Limpa caracteres especiais do nome dos campos
            delta = rename_columns(delta)

            # Inclui campos auxiliares de agrupamento dos Ramos Susep
            delta = add_ramo_susep(delta, ramos)

            # Deleta o campo RECORDMODE
            if 'RECORDMODE' in delta.columns:
                delta = delta.drop(delta.RECORDMODE)

            # Formato padrão de Data-Hora (Governança)
            delta = delta.withColumn('DT_INGESTAO_BRONZE',
                                     f.lit(f.from_utc_timestamp(f.current_timestamp(), "Brazil/East")))
            delta = format_date_time(delta, 'DT_INGESTAO_BRONZE')

            if 'ZCHAPOLIC' in delta.columns:
                delta = delta.withColumn('ZCHAPOLIC', f.lpad(delta.ZCHAPOLIC, 20, '0').alias('ZCHAPOLIC'))

            if 'ZCHSININT' in delta.columns:
                delta = delta.withColumn('ZCHSININT', f.col('ZCHSININT').cast('string').alias('ZCHSININT'))

            # Valida campos chaves nulos
            string_col = [item[0] for item in delta[chaves_list].dtypes if item[1].startswith('string')]
            int_col = [item[0] for item in delta[chaves_list].dtypes if item[1].endswith('int')]
            double_col = [item[0] for item in delta[chaves_list].dtypes if item[1].startswith('double')]
            decimal_col = [item[0] for item in delta[chaves_list].dtypes if item[1].startswith('decimal')]

            delta = delta.fillna('', subset=string_col) \
                .fillna(0, subset=int_col) \
                .fillna(0.0, subset=double_col) \
                .fillna(0.0, subset=decimal_col)

            # Se não tiver, inclui o campo ZCHREQUID com o mesmo valor do OHREQUID para igualar com o schema do full
            if 'ZCHREQUID' not in delta.columns:
                delta = delta.withColumn('ZCHREQUID', delta.OHREQUID)

            # Busca o maior REQUID e RECORD do delta para Update
            delta = max_delta(delta, chaves_list)

            # Obtém a tabela Full atual
            full = sf.read_delta_table(spark, output_path + '/' + item.path_sink)
            if full is not None:
                try:
                    # Merge da Full atual com o novo Delta
                    merge_delta(full, delta, chaves_list)
                except Exception as e:
                    cf.log_error(spark, item.table, e, output_path+'/log_errors')
            else:
                # Grava Delta completo caso não existam dados na Bronze
                sf.write_delta_file(delta, output_path + '/' + item.path_sink, 'append')

            env = Environment()
            sf.delete(dbutils, input_path + '/' + item.path_source + '.csv', env)

            # ============= VALIDAÇÕES DA CARGA =============#
            table_renamed = item.table.replace('ZOHOPIN', 'ZDAOPIN')

            try:
                cf.valida_volumetria(dbutils, spark,
                                  item.table,
                                  output_path + '/' + item.path_sink,
                                  output_path + '/validacao/cap/volumetria')

            except Exception as e:
                cf.log_error(spark, item.table, e, output_path+'/log_errors')
