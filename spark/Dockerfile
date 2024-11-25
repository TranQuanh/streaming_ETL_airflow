FROM docker.io/bitnami/spark:3.5
USER root
RUN mkdir -p /data && chown -R 1001:root /data
RUN mkdir -p /opt/bitnami/spark/.ivy2 && chown -R 1001:root /opt/bitnami/spark/.ivy2
USER 1001
VOLUME /data
VOLUME /opt/bitnami/spark/.ivy2
RUN pip install pyspark confluent_kafka venv-pack pyyaml ua-parser user-agents psycopg2-binary