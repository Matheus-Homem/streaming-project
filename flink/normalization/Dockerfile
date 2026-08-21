FROM flink:2.3.0-scala_2.12-java17

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

WORKDIR /opt/flink/usrlib

ENV PYTHONPATH=/opt/flink/usrlib:${PYTHONPATH}

COPY requirements/ ./requirements/
RUN pip install --no-cache-dir --break-system-packages -r requirements/flink.txt

COPY flink/common/ ./flink/common/
COPY flink/normalization/ ./flink/normalization/
COPY shared/ ./shared/

CMD ["standalone-job", "-py", "/opt/flink/usrlib/flink/normalization/app.py"]