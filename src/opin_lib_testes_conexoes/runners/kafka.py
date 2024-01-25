import sys

class KafkaTest:
    def __init__(self, context):
        self.context = context

    def _can_connect_kafka(self, topic):
        
        try:
            df = self.context.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.context.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe",  topic) \
                .option("kafka.security.protocol", self.context.KAFKA_SECURITY_PROTOCOL) \
                .option("kafka.sasl.jaas.config", self.context.KAFKA_SASL_JAAS_CONFIG) \
                .option("kafka.sasl.mechanism", self.context.KAFKA_SASL_MECHANISM).load()
        except Exception as e:
            print(f"Não foi possível conectar ao tópico {topic} ({self.context.KAFKA_BOOTSTRAP_SERVERS}) {e} ")
            return False
        if len(df.schema) > 0:
            print(f"Conectado ao tópico {topic} ({self.context.KAFKA_BOOTSTRAP_SERVERS})")
            return True
        
        print(f"Não foi possível conectar ao tópico {topic} ({self.context.KAFKA_BOOTSTRAP_SERVERS}) (Tópico não existe)")
        return False
        
    def topic_consent(self):
        return self._can_connect_kafka(self.context.KAFKA_TOPIC_CONSENTIMENTO)
    
    def topic_dados_complementares_pf(self):
        return self._can_connect_kafka(self.context.KAFKA_TOPIC_DADOS_COMPLEMENTARES_PF)
    
    def topic_dados_complementares_pj(self):
        return self._can_connect_kafka(self.context.KAFKA_TOPIC_DADOS_COMPLEMENTARES_PJ)
    
    def topic_representante_legal(self):
        return self._can_connect_kafka(self.context.KAFKA_TOPIC_REPRESENTANTE_LEGAL)
    

def test_kafka(context):
    tst = KafkaTest(context)
    tst.topic_consent()
    tst.topic_dados_complementares_pf()
    tst.topic_dados_complementares_pj()
    tst.topic_representante_legal()