FROM docker.io/openanolis/anolisos:8

RUN yum install -y libzip unzip wget cmake openssl-devel llvm clang-devel clang krb5-workstation git gcc gcc-c++

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
# Note: Specify the correct target for aarch64
RUN /rustup-init -y --default-toolchain nightly-2025-06-01-aarch64-unknown-linux-gnu

# install java
RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
# Note: Update JAVA_HOME path for aarch64
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc

# install protoc
# Note: Download the aarch64 version of protoc
RUN wget -O /protobuf-21.7-linux-aarch64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-aarch_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-aarch64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

# attach libjvm.so
# Note: Update the library path for aarch64
RUN echo 'export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64/server:${LD_LIBRARY_PATH}' >> ~/.bashrc

# setup hadoop env
RUN curl -LsSf https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5-aarch64.tar.gz | tar zxf - -C /root
RUN echo "export HADOOP_HOME=/root/hadoop-3.3.5" >> ~/.bashrc
RUN echo "export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)" >> ~/.bashrc
RUN echo "export HDRS_NAMENODE=default" >> ~/.bashrc
RUN echo "export HDRS_WORKDIR=/tmp/hdrs/" >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc