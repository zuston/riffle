FROM centos:7

RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i 's|^mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/CentOS-*.repo \
 && sed -i 's|^#baseurl=http://mirror.centos.org|baseurl=https://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo

# install common tools
RUN yum install -y centos-release-scl epel-release

# replace to tuna mirror repo
RUN sed -i 's|^mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/epel.repo \
 && sed -i 's|^#baseurl=http://download.fedoraproject.org/pub/epel|baseurl=https://mirrors.tuna.tsinghua.edu.cn/epel|g' /etc/yum.repos.d/epel.repo

RUN sed -i 's|^#baseurl=http://mirror.centos.org|baseurl=https://vault.centos.org|g' /etc/yum.repos.d/CentOS-SCLo-scl.repo
RUN sed -i 's|^mirrorlist=|#mirrorlist=|g' /etc/yum.repos.d/CentOS-SCLo-rh.repo \
 && sed -i 's|^#baseurl=http://mirror.centos.org|baseurl=https://vault.centos.org|g' /etc/yum.repos.d/CentOS-SCLo-rh.repo

RUN yum install -y epel-release \
 && yum install -y libzip unzip wget cmake3 openssl-devel llvm clang-devel clang krb5-workstation clang-devel git gcc

# install gcc-8
#RUN yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++
#RUN echo '. /opt/rh/devtoolset-8/enable' >> ~/.bashrc

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
RUN echo 'export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> ~/.bashrc

# install maven
# RUN yum install -y rh-maven35
# RUN echo 'source /opt/rh/rh-maven35/enable' >> ~/.bashrc

# install protoc
RUN wget -O /protobuf-21.7-linux-x86_64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-x86_64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-x86_64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

# attach libjvm.so
RUN echo 'export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}' >> ~/.bashrc

# setup hadoop env
RUN curl -LsSf https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz | tar zxf - -C /root
RUN echo "export HADOOP_HOME=/root/hadoop-3.3.5" >> ~/.bashrc
RUN echo "export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)" >> ~/.bashrc
RUN echo "export HDRS_NAMENODE=default" >> ~/.bashrc
RUN echo "export HDRS_WORKDIR=/tmp/hdrs/" >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc
