import re
import pyspark.sql.functions as f


def clean_special_char(df):
    replace_cols = []
    for field in df.schema.fields:
        replace_cols.append(field.name)

    df = df.select([f.translate(f.col(c).cast('string'), "[]'", "").alias(c) for c in replace_cols])

    return df


def format_date_time(df, column):
    return df.withColumn(column, f.date_format(column, "yyyy-MM-dd HH:mm:ss").alias(column))


def rename_columns(df):
    for name in df.schema.names:
        df = df.withColumnRenamed(name, re.sub(r'(^_|_$)', '', name.replace('/BIC/', '')))

    return df


def add_ramo_susep(df, ramos):
    if 'ZCHRAMO' in df.columns:
        df = df.withColumn('ZCHRAMO', df.ZCHRAMO.cast('int').alias('ZCHRAMO'))
        df = df.join(ramos, df.ZCHRAMO == ramos.CRAMO, "left")
        df = df.drop(df.CRAMO)

    return df
