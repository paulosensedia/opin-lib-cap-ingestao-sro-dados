import sys
from pymongo import MongoClient
from time import sleep

class CosmosTest:
    def __init__(self, context):
        self.context = context

    @staticmethod
    def can_connect_cosmos(client, function_name):
        sleep(5)
        if len(client.topology_description.readable_servers) > 0:
            print(f"Conexão com CosmosDB OK: {function_name.replace('_','-')} \n{client.topology_description.server_descriptions()}")
            return True
        print(f"Falha no teste de conexão: {function_name.replace('_','-')} \n{client.topology_description.server_descriptions()}")
        return False

    def azu_bscosmoscadastral(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_CADASTRAL)
        return CosmosTest.can_connect_cosmos(client, function_name)
    
    def azu_bscosmoscadastrocomplementar(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_DADOS_COMPLEMENTARES)
        return CosmosTest.can_connect_cosmos(client, function_name)
    
    def azu_bscosmosrepresentantelegal(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_REPRESENTANTE_LEGAL)
        return CosmosTest.can_connect_cosmos(client, function_name)

    def azu_bsuri_cosmosdb_canaisatend(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_CANAIS_ATEND)
        return CosmosTest.can_connect_cosmos(client, function_name)
       
    def azpcscosmosauto(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_AUTO)
        return CosmosTest.can_connect_cosmos(client, function_name)

    def azpcscosmosautotransacional(self):
        function_name = sys._getframe().f_code.co_name
        client = MongoClient(self.context.COSMOSDB_URI_AUTO_TRANSACIONAL)
        return CosmosTest.can_connect_cosmos(client, function_name)


def test_cosmos(context):
    tst = CosmosTest(context)
    tst.azu_bscosmoscadastral()
    tst.azu_bscosmoscadastrocomplementar()
    tst.azu_bscosmosrepresentantelegal()
    return True