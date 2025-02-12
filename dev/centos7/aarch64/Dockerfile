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
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2024-10-01-aarch64-unknown-linux-gnu

# install protoc
RUN wget -O /protobuf-21.7-linux-aarch64.zip https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protoc-21.7-linux-aarch64.zip
RUN mkdir /protobuf-bin && (cd /protobuf-bin && unzip /protobuf-21.7-linux-aarch64.zip)
RUN echo 'export PATH="$PATH:/protobuf-bin/bin"' >> ~/.bashrc

RUN echo "export RUST_BACKTRACE=1" >> ~/.bashrc
