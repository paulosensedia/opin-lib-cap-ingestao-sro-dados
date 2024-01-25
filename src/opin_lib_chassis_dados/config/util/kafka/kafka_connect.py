from abc import ABC, abstractmethod

from opin_lib_chassis_dados.config.context import Context


class KafkaConnect(ABC):
    """
    Classe responsável por fornecer uma abstração de classe contexto Kafka, com
    o intuito transparecer a leitura de tópicos para o ambiente em que a
    aplicação é executada.
    """

    @abstractmethod
    def get_connection(self, context: Context):
        pass
