FROM sonarsource/sonar-scanner-cli:latest

ARG SONAR_LOGIN_USER
ARG SONAR_LOGIN_PASSWORD
ARG SONAR_URL
ARG PROJECT_NAME

WORKDIR /app
COPY . /app

ENV PYTHONPATH=/home/lkellermann/Documents/repos/opin-lib-testes-conexoes-chassi/opin-lib-chassis-dados/src/opin_lib_chassis_dados

RUN pip install pyspark wheel
RUN pip install pytest-cov
RUN pip install pyspark_test
RUN pip install databricks-test
RUN pip install jproperties
RUN pip install pymongo
RUN pip install delta-spark
RUN pip install findspark
RUN pip install tenacity

RUN pytest --cov=src test/ --cov-report=xml
RUN pwd /app

RUN sonar-scanner \
    -Dsonar.projectKey=$PROJECT_NAME \
    -Dsonar.login=$SONAR_LOGIN_USER \
    -Dsonar.password=$SONAR_LOGIN_PASSWORD \
    -Dsonar.host.url=$SONAR_URL