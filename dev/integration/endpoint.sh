#!/bin/bash
set -e

# Get the role/command from the first argument
ROLE=${1:-run-tests}

# Source environment variables
source ~/.bashrc

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo_role() {
    echo -e "${BLUE}[ROLE: $ROLE]${NC} $1"
}

wait_for_shuffle_servers() {
    if [ "${COORDINATOR_TYPE:-uniffle}" = "riffle" ]; then
        echo_info "Waiting for Riffle coordinator and shuffle servers..."
        for i in {1..60}; do
            if nc -z "${COORDINATOR_HOST}" 21000 >/dev/null 2>&1 && \
                curl -fsS "http://${RIFFLE_SERVER_1_HOST}:19998/metrics" >/dev/null && \
                curl -fsS "http://${RIFFLE_SERVER_2_HOST}:19999/metrics" >/dev/null; then
                # ponytail: fixed wait replaces a gRPC registration probe; increase if the heartbeat interval changes.
                sleep 5
                echo_info "Riffle coordinator and shuffle servers are ready."
                return 0
            fi
            sleep 2
        done

        echo_error "Timed out waiting for Riffle coordinator and shuffle servers."
        exit 1
    fi

    local endpoint="http://${COORDINATOR_HOST}:19995/api/server/nodes?status=ACTIVE"

    echo_info "Waiting for two active shuffle servers..."
    for i in {1..60}; do
        if curl -fsS "$endpoint" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
servers = payload.get("data", [])
raise SystemExit(0 if len(servers) >= 2 else 1)
'; then
            echo_info "Two active shuffle servers are registered."
            return 0
        fi
        sleep 2
    done

    echo_error "Timed out waiting for active shuffle servers."
    curl -fsS "$endpoint" || true
    exit 1
}

wait_for_coordinator() {
    echo_info "Waiting for coordinator ${COORDINATOR_HOST}:21000..."
    for i in {1..60}; do
        if nc -z "${COORDINATOR_HOST}" 21000 >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done

    echo_error "Timed out waiting for coordinator ${COORDINATOR_HOST}:21000."
    exit 1
}

build_riffle_binaries() {
    if [ ! -f /riffle/target/debug/riffle-server ] || \
        [ ! -f /riffle/target/debug/riffle-coordinator ]; then
        echo_info "Building Riffle binaries..."
        cd /riffle
        cargo build --bin riffle-server --bin riffle-coordinator
    fi
}

# ============================================================================
# Role-based service startup
# ============================================================================

echo_role "Starting as: $ROLE"

case "$ROLE" in
  coordinator)
    # ========== Uniffle Coordinator ==========
    echo_info "Creating coordinator directories..."
    mkdir -p ${UNIFFLE_HOME}/logs
    
    echo_info "Starting Uniffle Coordinator..."
    cd ${UNIFFLE_HOME}
    
    echo_info "Starting coordinator..."
    /bin/bash ./bin/start-coordinator.sh || {
        echo_error "Failed to start coordinator"
        exit 1
    }
    
    # Wait for coordinator to initialize and check health
    echo_info "Waiting for coordinator to be ready..."
    for i in {1..30}; do
        if curl -f http://localhost:19995/api/app/total >/dev/null 2>&1; then
            echo_info "Coordinator is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            echo_error "Coordinator failed to start after 60 seconds"
            exit 1
        fi
        sleep 2
    done
    
    # Keep container running by tailing coordinator logs
    if [ -f ${UNIFFLE_HOME}/logs/coordinator.log ]; then
        echo_info "Tailing coordinator logs..."
        exec tail -f ${UNIFFLE_HOME}/logs/coordinator.log
    else
        echo_warn "Coordinator log file not found, keeping container alive..."
        exec tail -f /dev/null
    fi
    ;;

  riffle-coordinator)
    echo_info "Starting Riffle Coordinator..."
    exec /riffle/target/debug/riffle-coordinator \
        --config "${RIFFLE_HOME}/conf/riffle-coordinator.conf"
    ;;

  riffle-compile)
    build_riffle_binaries
    exec tail -f /dev/null
    ;;

  riffle-server-1)
    echo_info "Starting Riffle Server 1..."
    COORDINATOR_HOST=${COORDINATOR_HOST:-uniffle-coordinator}
    mkdir -p /tmp/riffle-server-1/data
    cd /tmp/riffle-server-1
    cp ${RIFFLE_HOME}/conf/riffle.conf.1 config.toml
    sed -i "s|uniffle-coordinator:21000|${COORDINATOR_HOST}:21000|" config.toml
    mkdir -p /tmp/riffle-server-1/log
    wait_for_coordinator

    exec nohup /riffle/target/debug/riffle-server --config config.toml &
    sleep 5
    echo_info "Tailing logs:"
    exec tail -f /tmp/riffle-server-1/log/riffle-server.0
    ;;

  riffle-server-2)
    echo_info "Starting Riffle Server 2..."
    COORDINATOR_HOST=${COORDINATOR_HOST:-uniffle-coordinator}
    mkdir -p /tmp/riffle-server-2/data
    cd /tmp/riffle-server-2
    cp ${RIFFLE_HOME}/conf/riffle.conf.2 config.toml
    sed -i "s|uniffle-coordinator:21000|${COORDINATOR_HOST}:21000|" config.toml
    mkdir -p /tmp/riffle-server-2/log
    wait_for_coordinator

    exec nohup /riffle/target/debug/riffle-server --config config.toml &
    sleep 5
    echo_info "Tailing logs:"
    exec tail -f /tmp/riffle-server-2/log/riffle-server.0
    ;;

  spark-client)
    COORDINATOR_HOST=${COORDINATOR_HOST:-uniffle-coordinator}
    echo_info "==========================================="
    echo_info "Spark Client is ready. Services available:"
    echo_info "  - Coordinator: ${COORDINATOR_HOST}:21000"
    echo_info "  - Riffle Server 1: http://riffle-server-1:19998"
    echo_info "  - Riffle Server 2: http://riffle-server-2:19999"
    echo_info "  - Spark Home: ${SPARK_HOME}"
    echo_info "==========================================="
    echo_info "To run Spark Shell:"
    echo_info "    ${SPARK_HOME}/bin/spark-shell --master local[*]"
    echo_info ""
    echo_info "To run Spark SQL:"
    echo_info "    ${SPARK_HOME}/bin/spark-sql --master local[*]"
    echo_info "==========================================="

    # Keep the container running
    exec tail -f /dev/null
    ;;

  run-tests)
    # ========== Run Full Integration Tests ==========
    echo_info "Waiting for Riffle Servers to be ready..."
    COORDINATOR_HOST=${COORDINATOR_HOST:-uniffle-coordinator}
    COORDINATOR_TYPE=${COORDINATOR_TYPE:-uniffle}
    RIFFLE_SERVER_1_HOST=${RIFFLE_SERVER_1_HOST:-riffle-server-1}
    RIFFLE_SERVER_2_HOST=${RIFFLE_SERVER_2_HOST:-riffle-server-2}
    wait_for_shuffle_servers

    # Run Spark SQL Integration Test
    echo_info "Running basic test..."
    cd ${SPARK_HOME}

    # case1: with sql_set sqls
    if ./bin/spark-shell \
        --master local[1] \
        --conf "spark.rss.coordinator.quorum=${COORDINATOR_HOST}:21000" \
        -i /tmp/sql_set/basic.scala; then
        echo_info "Spark SQL test completed successfully!"
    else
        echo_error "Spark SQL test failed!"
        exit 1
    fi

    # case2: run tpcds sqls
    echo_info "Merging all TPCDS SQLs into a single file..."
    MERGED_SQL="/tmp/tpcds_sqls.sql"
    echo "USE tpcds.sf1;" > "$MERGED_SQL"

    for sql_file in /tmp/sql_set/*.sql; do
        cat "$sql_file" >> "$MERGED_SQL"
        echo ";" >> "$MERGED_SQL"
    done

    echo_info "Running all TPCDS SQL..."
    start_time=$(date +%s)
    if ./bin/spark-sql \
        --master local[1] \
        --conf "spark.rss.coordinator.quorum=${COORDINATOR_HOST}:21000" \
        -f "$MERGED_SQL"; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo_info "All SQL files executed in one session successfully (Time: ${duration}s)"
    else
        echo_error "Execution of merged SQL file failed!"
        exit 1
    fi

    echo_info "==========================================="
    echo_info "All tests passed successfully!"
    echo_info "==========================================="
    ;;

  *)
    echo_error "Unknown role: $ROLE"
    echo_info "Available roles:"
    echo_info "  - coordinator: Start Uniffle Coordinator"
    echo_info "  - riffle-coordinator: Start Riffle Coordinator"
    echo_info "  - riffle-server-1: Start Riffle Server 1"
    echo_info "  - riffle-server-2: Start Riffle Server 2"
    echo_info "  - spark-client: Start Spark client (interactive)"
    echo_info "  - run-tests: Run full integration tests"
    exit 1
    ;;
esac
