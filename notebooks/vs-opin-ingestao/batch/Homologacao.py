# Databricks notebook source
def Ler_Bronze(base, tabela):
    try:
        df = spark.read.format("delta").load('/mnt/bronze/ingestao_dados/sro/' + base + '/' + tabela)
        c = df.count()
    except:
        c = 0

    if c > 0:
        print("Quantidade de registros Full: " + c)
        df.display()
    else:
        print('Não há registros para a tabela ' + tabela)


def Ler_Transient_Full(base, tabela):
    try:
        df = sqlContext.read.load('/mnt/transient/ingestao_dados/sro/full/' + base + '/' + tabela + '.csv',
                                  format='csv', header='true', inferSchema='true', delimiter='|')
        c = df.count()
    except:
        c = 0

    if c > 0:
        print("Quantidade de registros Full: ", c)
        df.display()
    else:
        print('Não há registros para a tabela', tabela)


def Ler_Transient_Delta(base, tabela):
    try:
        df = sqlContext.read.load('/mnt/transient/ingestao_dados/sro/delta/' + base + '/' + tabela + '.csv',
                                  format='csv', header='true', inferSchema='true', delimiter='|')
        c = df.count()
    except:
        c = 0

    if c > 0:
        print("Quantidade de registros Full: ", c)
        df.display()
    else:
        print('Não há registros para a tabela', tabela)


# COMMAND ----------

# DBTITLE 1,Bronze - Informar o nome da base (premio ou sinistro) e o nome da tabela a consultar:
Ler_Bronze(base='premio', tabela='ZDAOPIN02')

# COMMAND ----------

# DBTITLE 1,Transient Full - Informar o nome da base (premio ou sinistro) e o nome da tabela a consultar:
Ler_Transient_Full(base='premio', tabela='ZOHOPIF02')

# COMMAND ----------

# DBTITLE 1,Tabela da Transient Delta
Ler_Transient_Delta(base='premio', tabela='ZOHOPIN02')

# COMMAND ----------

# DBTITLE 1,Bronze - Filtros
base = 'premio'
tabela = 'ZDAOPIN09'
df = spark.read.format("delta").load('/mnt/bronze/ingestao_dados/sro/' + base + '/' + tabela)

df.where("ZCHREQUID = 341375 and ZDTMOVIME = 20220609 and ZCHAPOLIC = 590 ").display()






