from datetime import datetime
from enum import Enum


class TestDataGenericValuesEnum(Enum):
    ID_PRODUTO = 1234567
    NOME_PRODUTO = 'Teste Veiculo'
    ID_TIPO_PRODUTO = 1
    TIPO_TIPO_PRODUTO = 'TIPOA'
    PROCEDENCIA = 'PROCEENCIA A'
    DATA = datetime(2023, 12, 15)
    LIMITE_MIN_IDADE_ANOS = 60
    LIMITE_MAX_IDADE_ANOS = 60
    CEP_INICIO = '89220000'
    CEP_FIM = '89220000'
    ZCH_RAMO = '10'


class TestDataRamosValuesEnum(Enum):
    ID_RAMO = 1
    C_RAMO = 10
    DESC_RAMO = 'Descrição Ramo'
    R_RAMO = 'RAMO'
    R_GRP_RAMO = 'G1'