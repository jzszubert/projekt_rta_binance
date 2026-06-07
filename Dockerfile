# Nowoczesna i wspierana baza
FROM quay.io/jupyter/pyspark-notebook:python-3.11
# FROM quay.io/jupyter/pyspark-notebook:x86_64-spark-3.5.0

USER root

# Instalacja niezbędnych narzędzi systemowych
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl vim wget git netcat-openbsd graphviz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalacja binarnej wersji Kafki (do narzędzi CLI w terminalu Jupytera)
ENV KAFKA_VERSION=3.7.1
ENV SCALA_VERSION=2.12
RUN wget -q https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /usr/local/ && \
    ln -s /usr/local/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /usr/local/kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    rm -rf /usr/local/kafka/bin/windows /usr/local/kafka/site-docs
ENV PATH="$PATH:/usr/local/kafka/bin"
# Instalacja Python packages (bez Torcha)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
    ipywidgets==8.1.5 \
    flask==3.0.3 \
    litserve==0.2.7 \
    pydot==3.0.2 \
    pennylane==0.40.0 \
    pennylane-lightning==0.40.0 \
    pandas==2.2.3 \
    scipy==1.15.2 \
    matplotlib==3.10.0 \
    sympy==1.13.3 \
    seaborn==0.13.2 \
    redis==5.0.1 \
    streamlit==1.38.0 \
    plotly==6.8.0 \
    psycopg2-binary==2.9.12 \
    confluent-kafka==2.8.0 \
    kafka-python-ng && \
    fix-permissions "/home/${NB_USER}"

USER ${NB_USER}
WORKDIR /home/jovyan/notebooks

