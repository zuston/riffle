ARG INTEGRATION_BASE_IMAGE=riffle-integration-base:local
FROM ${INTEGRATION_BASE_IMAGE}

COPY coordinator.conf /opt/uniffle/conf/coordinator.conf
COPY riffle-coordinator.conf /opt/riffle/conf/riffle-coordinator.conf
COPY riffle.conf.1 /opt/riffle/conf/riffle.conf.1
COPY riffle.conf.2 /opt/riffle/conf/riffle.conf.2
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY sql_set /tmp/sql_set
COPY endpoint.sh /usr/local/bin/endpoint.sh
RUN chmod +x /usr/local/bin/endpoint.sh
ENTRYPOINT ["/usr/local/bin/endpoint.sh"]
