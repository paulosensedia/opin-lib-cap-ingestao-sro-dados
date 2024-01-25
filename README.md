# Introdução
O objetivo desse projeto é servir de estrutura inicial para a criação de novos 
projetos de dados. A fim de exemplo para esse projeto trabalharemos com o 
seguinte [conjunto de dados](https://files.grouplens.org/datasets/movielens/ml-25m.zip), 
que trata-se de informações de filmes. 

Referências:
- https://grouplens.org
- https://movielens.org
- https://grouplens.org/datasets/movielens/25m

# Ferramentas de desenvolvimento
Utilizamos as seguintes ferramentas para desenvolvimento de aplicações. Considere 
instalá-los conforme manual do fornecedor:
- [Docker](https://www.docker.com)
- [Docker Compose](https://docs.docker.com/compose/install)
- [PyCharm](https://www.jetbrains.com/pycharm/download)
- [Python 3.8](https://www.python.org)
- [Java 8](https://www.java.com)

# Configurações do ambiente
## Ambiente Linux
### Atualizando o PIP
1. Abra o terminal
2. Execute o seguinte comando: `python -m pip install --upgrade pip`

## Ambiente Windows
### Configurando o HadoopWin (Apache Hadoop)
1. Criar o seguinte diretório: <code>C:\\devtools\\</code>
2. Efetuar download do arquivo [hadoop-3.3.1.tar.gz ](https://dlcdn.apache.org/hadoop/common/current/hadoop-3.3.1.tar.gz)
3. Descompactar o arquivo <code>tar.gz</code> no diretório acima (item 1)
4. No menu Iniciar, procurar por "Variáveis de ambiente"
5. Configurar o seguinte em "Variáveis de usuário":
    - Clicar no botão "Novo"
    - Em "Nome da variável" preencha: <code>HADOOP_HOME</code>
    - Em "Valor da variável" preencha: <code>C:\devtools\hadoop-3.3.1</code>
    - Clicar no botão "Ok"
6. Configurar o seguinte em "Variáveis de ambiente do usuário":
    - Selecionar a variável <code>Path</code>
    - Clicar no botão "Editar"
    - Clicar no botão "Novo"
    - Insira o valor <code>%HADOOP_HOME%\bin</code>
7. Copiar o arquivo abaixo para dentro da pasta <code>C:\\devtools\\</code>:
    - <code>opin-lib-chassis-dados/infra/local/windows/hadoop/winutils.exe</code>

# Configurações do projeto
## Estrutura de Datalake
1. Efetue download do conjunto de dados e disponibilize-os na seguinte estrutura de diretórios, 
a partir da pasta raiz do Sistema Operacional. 
- `/temporary/external/movielens/genome_scores/genome-scores.csv`
- `/temporary/external/movielens/genome_tags/genome-tags.csv`
- `/temporary/external/movielens/links/links.csv`
- `/temporary/external/movielens/movies/movies.csv`
- `/temporary/external/movielens/ratings/ratings.csv`
- `/temporary/external/movielens/tags/tags.csv`
- `/storage/transient`
- `/storage/bronze`
- `/storage/silver`
- `/storage/gold`

2. Certifique-se de conceder permissão de escrita e leitura ao usuário sistêmico atual.

## Docker Kafka
### Iniciando o serviço Kafka
1. Acesse o terminal do Sistema Operacional
2. Acesse a pasta do projeto: `infra/local/docker/kafka`
3. Inicie o serviço Kafka: `sudo docker-compose up -d --build`
4. Liste os serviços em execução: `sudo docker-compose ps`
5. Verifique se os serviços `broker` e `zookeeper` estão "UP":

```sh
Name        State
broker      Up
zookeeper   Up
```
### Acessando o bash do Broker Kafka
1. Certifique-se de que o serviço Kafka esteja em execução.
2. Acesse o bash do Broker Kafka: `sudo docker exec -it broker /bin/sh`

### Criando Tópico Kafka
1. Acesse o bash do Broker Kafka
2. Criar os seguinte tópicos:
- `kafka-topics --create --topic MOVIELENS_GENOME_SCORES --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
- `kafka-topics --create --topic MOVIELENS_GENOME_TAGS --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
- `kafka-topics --create --topic MOVIELENS_LINKS --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
- `kafka-topics --create --topic MOVIELENS_MOVIES --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
- `kafka-topics --create --topic MOVIELENS_RATINGS --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
- `kafka-topics --create --topic MOVIELENS_TAGS --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
3. Verificar se os tópicos foram criados: `kafka-topics --list --bootstrap-server localhost:9092`

Abaixo comandos úteis para utilização do Kafka pelo terminal
1. Acessar o bash do Broker Kafka: `sudo docker exec -it broker /bin/sh`
2. Criar tópico: `kafka-topics --create --topic MY_TOPIC_TEST --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
3. Detalhar tópico: `kafka-topics --describe --bootstrap-server localhost:9092 --topic MY_TOPIC_TEST`
4. Criar producer: `kafka-console-producer --topic MY_TOPIC_TEST --bootstrap-server localhost:9092`
5. Criar consumer: `kafka-console-consumer --topic MY_TOPIC_TEST --from-beginning --bootstrap-server localhost:9092`
6. Deletar tópico: `kafka-topics --delete --bootstrap-server localhost:9092 --topic MY_TOPIC_TEST`

### Executando processo de Streaming com Kafka (Exemplo)
1. Acesse o bash do Broker Kafka: `sudo docker exec -it broker /bin/sh`
2. Crie o seguinte producer: `kafka-console-producer --topic MOVIELENS_GENOME_SCORES --bootstrap-server localhost:9092`
3. Insira a seguinte mensagem: `{"movieId": "1", "tagId": "94", "relevance": "0.36224999999999996", "dbaseOperationIndicator": "insert"}`
4. No PyCharm, execute o script: `notebooks\vs-opin-fornecimento\movielens\streaming\ingestion\genome_score_ingestion.py` 
5. Certifique-se de que a mensagem foi consumida do tópico Kafka e salva na camada Bronze.
6. No PyCharm, execute o script: `notebooks\vs-opin-fornecimento\movielens\streaming\transformation\genome_score_transformation.py` 
5. Certifique-se de que a mensagem foi consumida da camada Bronze e escrita (upsert) na camada Silver. 