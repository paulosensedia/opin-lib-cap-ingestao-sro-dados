import pyspark.sql.functions as f
from opin_lib_cap_ingestao_sro_dados import common_functions as cf
from opin_lib_cap_ingestao_sro_dados.config.util.environment import Environment
from opin_lib_cap_ingestao_sro_dados import storage_functions as sf
from opin_lib_cap_ingestao_sro_dados.functions.transformation.transformation_functions import rename_columns, add_ramo_susep, \
    format_date_time


def get_tables_name_full(dbutils, spark, path):
    tables = sf.read_csv_file(dbutils, spark, '|', path, True)
    tables = tables.withColumn('path_source', f.concat(f.col('base'), f.lit('/'), f.col('table')))
    tables = tables.withColumn('path_sink',
                               f.concat(f.col('base'), f.lit('/'), f.regexp_replace('table', 'ZOHOPCF', 'ZDAOPCP')))

    return tables


def ingestion(dbutils, spark, context, ramos_path, tables_path, input_path, output_path):
    ramos = sf.read_delta_file(dbutils, spark, ramos_path)
    tables = get_tables_name_full(dbutils, spark, tables_path + '.csv')

    for item in tables.collect():
        df = sf.read_csv_file(dbutils, spark, '|', input_path + '/' + item.path_source + '.csv', True)

        if (df is not None) and (df.count() > 0):
            chaves_list = list(item.chaves.split(", "))

            # Limpa caracteres especiais do nome dos campos
            df = rename_columns(df)

            # Inclui campos auxiliares de agrupamento dos Ramos Susep se o código existir na tabela
            df = add_ramo_susep(df, ramos)

            # Deleta o campo RECORDMODE
            if 'RECORDMODE' in df.columns:
                df = df.drop(df.RECORDMODE)

            # Inclui o campo ZCHREQUID com o mesmo valor do OHREQUID para igualar com o schema do delta
            df = df.withColumn('ZCHREQUID', df.OHREQUID)

            # Formato padrão de Data-Hora (Governança)
            df = df.withColumn('DT_INGESTAO_BRONZE', f.lit(f.from_utc_timestamp(f.current_timestamp(), "Brazil/East")))
            df = format_date_time(df, 'DT_INGESTAO_BRONZE')

            if 'ZCHAPOLIC' in df.columns:
                df = df.withColumn('ZCHAPOLIC', f.lpad(df.ZCHAPOLIC, 20, '0').alias('ZCHAPOLIC'))

            if 'ZCHSININT' in df.columns:
                df = df.withColumn('ZCHSININT', f.col('ZCHSININT').cast('string').alias('ZCHSININT'))

            # Valida campos chaves nulos
            string_col = [item[0] for item in df[chaves_list].dtypes if item[1].startswith('string')]
            int_col = [item[0] for item in df[chaves_list].dtypes if item[1].endswith('int')]
            double_col = [item[0] for item in df[chaves_list].dtypes if item[1].startswith('double')]
            decimal_col = [item[0] for item in df[chaves_list].dtypes if item[1].startswith('decimal')]

            df = df.fillna('', subset=string_col) \
                .fillna(0, subset=int_col) \
                .fillna(0.0, subset=double_col) \
                .fillna(0.0, subset=decimal_col)

            sf.write_delta_file(df, output_path + '/' + item.path_sink, 'overwrite')
            sf.delete(dbutils, input_path + '/' + item.path_source + '.csv', env=Environment())

            # ============= VALIDAÇÕES DA CARGA =============#
            table_renamed = item.table.replace('ZOHOPIF', 'ZDAOPIN')

            try:
                cf.valida_volumetria(dbutils, spark,
                                  item.table,
                                  output_path + '/' + item.path_sink,
                                  output_path + '/validacao/cap/volumetria')

                #cf.valida_obrigatoriedade(dbutils, spark,
                #                          chaves_list,
                #                          context.STORAGE_TRANSIENT_INGESTAO_SRO + '/validacao/cap/' + table_renamed + '.csv',
                #                          output_path + '/' + item.path_sink,
                #                          output_path + '/validacao/cap/obrigatoriedade/' + table_renamed)

                #cf.valida_tipagem(dbutils, spark,
                #                  item.table,
                #                  context.STORAGE_TRANSIENT_INGESTAO_SRO + '/validacao/cap/' + table_renamed + '.csv',
                #                  output_path + '/' + item.path_sink,
                #                  output_path + '/validacao/cap/tipagem')

            except Exception as e:
                cf.log_error(spark, item.table, e, output_path+'/log_errors')
