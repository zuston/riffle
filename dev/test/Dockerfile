FROM centos:7

# install common tools
RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum update -y
RUN yum install -y centos-release-scl epel-release
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum install -y libzip unzip wget cmake3 openssl-devel llvm clang-devel clang krb5-workstation clang-devel git gcc

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

RUN yum install -y llvm-toolset-7
RUN echo '. /opt/rh/llvm-toolset-7/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly-2025-06-01
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc

# install maven
# RUN yum install -y rh-maven35
# RUN echo 'source /opt/rh/rh-maven35/enable' >> ~/.bashrc

# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
ENV PATH="/protobuf-bin/bin:${PATH}"
RUN echo 'export PATH="/protobuf-bin/bin:$PATH"' >> ~/.bashrc

# attach libjvm.so
RUN echo 'export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}' >> ~/.bashrc

# setup hadoop env
RUN curl -LsSf https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz | tar zxf - -C /root
ENV HADOOP_HOME=/root/hadoop-3.3.5
RUN echo "export HADOOP_HOME=${HADOOP_HOME}" >> ~/.bashrc
RUN echo "export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)" >> ~/.bashrc
RUN echo "export HDRS_NAMENODE=default" >> ~/.bashrc
RUN echo "export HDRS_WORKDIR=/tmp/hdrs/" >> ~/.bashrc

# install python and pip
RUN yum install -y python3 python3-pip
RUN pip3 install --upgrade pip

# install netcat for port checking
RUN yum install -y nc

# install spark
ARG SPARK_VERSION=3.5.0
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# install pyspark
RUN pip3 install pyspark==${SPARK_VERSION}

# install uniffle coordinator (must be before configuring it)
ARG UNIFFLE_VERSION=0.10.0
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/uniffle/${UNIFFLE_VERSION}/apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz && \
    tar -xzf apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz --warning=no-unknown-keyword 2>&1 | grep -v "Ignoring unknown extended header keyword" || true && \
    EXTRACTED_DIR=$(ls -d apache-uniffle-* 2>/dev/null | head -1) && \
    if [ -n "$EXTRACTED_DIR" ]; then mv "$EXTRACTED_DIR" uniffle; fi && \
    rm -f apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz
ENV UNIFFLE_HOME=/opt/uniffle
ENV PATH="${UNIFFLE_HOME}/bin:${PATH}"

# download uniffle spark client
RUN mkdir -p ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/uniffle/rss-client-spark3-shaded/${UNIFFLE_VERSION}/rss-client-spark3-shaded-${UNIFFLE_VERSION}.jar \
    -O ${SPARK_HOME}/jars/rss-client.jar || \
    echo "Failed to download Uniffle Spark Client"

# setup riffle home directory
ENV RIFFLE_HOME=/opt/riffle
RUN mkdir -p ${RIFFLE_HOME}/conf

# configure riffle servers (place configs in RIFFLE_HOME/conf)
COPY riffle.conf.1 ${RIFFLE_HOME}/conf/riffle.conf.1
COPY riffle.conf.2 ${RIFFLE_HOME}/conf/riffle.conf.2

# configure spark to use riffle
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# configure uniffle coordinator (after UNIFFLE_HOME is set and uniffle is installed)
RUN mkdir -p ${UNIFFLE_HOME}/conf
COPY coordinator.conf ${UNIFFLE_HOME}/conf/coordinator.conf

# copy endpoint script
COPY endpoint.sh /usr/local/bin/endpoint.sh
RUN chmod +x /usr/local/bin/endpoint.sh

# setup environment variables
RUN echo "export SPARK_HOME=${SPARK_HOME}" >> ~/.bashrc && \
    echo "export UNIFFLE_HOME=${UNIFFLE_HOME}" >> ~/.bashrc && \
    echo "export RIFFLE_HOME=${RIFFLE_HOME}" >> ~/.bashrc && \
    echo "export PYTHONPATH=\${SPARK_HOME}/python:\${SPARK_HOME}/python/lib/py4j-*.zip:\${PYTHONPATH}" >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc

# Set entrypoint (no WORKDIR needed as endpoint.sh handles paths)
ENTRYPOINT ["/usr/local/bin/endpoint.sh"]
