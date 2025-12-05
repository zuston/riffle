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
    exec ./bin/start-coordinator.sh
    ;;

  riffle-server-1)
    # ========== Riffle Server 1 ==========
    echo_info "Waiting for coordinator to be ready..."
    COORDINATOR_HOST=${COORDINATOR_HOST:-coordinator}
    for i in {1..60}; do
        if nc -z $COORDINATOR_HOST 21000 2>/dev/null; then
            echo_info "Coordinator is ready!"
            break
        fi
        if [ $i -eq 60 ]; then
            echo_error "Coordinator not available after 120 seconds"
            exit 1
        fi
        echo "Waiting for coordinator... ($i/60)"
        sleep 2
    done

    # Build Riffle Server if not exists
    if [ ! -f /riffle/target/release/riffle-server ]; then
        echo_info "Building Riffle Server..."
        cd /riffle
        cargo build --release
    fi

    echo_info "Starting Riffle Server 1..."
    mkdir -p /tmp/riffle-server-1/data
    cd /tmp/riffle-server-1
    cp ${RIFFLE_HOME}/conf/riffle.conf.1 config.toml
    
    # Update coordinator address if needed
    if [ "$COORDINATOR_HOST" != "127.0.0.1" ] && [ "$COORDINATOR_HOST" != "localhost" ]; then
        sed -i "s/127.0.0.1:21000/${COORDINATOR_HOST}:21000/g" config.toml
    fi
    
    echo_info "Starting with config:"
    cat config.toml
    exec env RUST_LOG=info /riffle/target/release/riffle-server --config config.toml
    ;;

  riffle-server-2)
    # ========== Riffle Server 2 ==========
    echo_info "Waiting for coordinator to be ready..."
    COORDINATOR_HOST=${COORDINATOR_HOST:-coordinator}
    for i in {1..60}; do
        if nc -z $COORDINATOR_HOST 21000 2>/dev/null; then
            echo_info "Coordinator is ready!"
            break
        fi
        if [ $i -eq 60 ]; then
            echo_error "Coordinator not available after 120 seconds"
            exit 1
        fi
        echo "Waiting for coordinator... ($i/60)"
        sleep 2
    done

    # Build Riffle Server if not exists
    if [ ! -f /riffle/target/release/riffle-server ]; then
        echo_info "Building Riffle Server..."
        cd /riffle
        cargo build --release
    fi

    echo_info "Starting Riffle Server 2..."
    mkdir -p /tmp/riffle-server-2/data
    cd /tmp/riffle-server-2
    cp ${RIFFLE_HOME}/conf/riffle.conf.2 config.toml
    
    # Update coordinator address if needed
    if [ "$COORDINATOR_HOST" != "127.0.0.1" ] && [ "$COORDINATOR_HOST" != "localhost" ]; then
        sed -i "s/127.0.0.1:21000/${COORDINATOR_HOST}:21000/g" config.toml
    fi
    
    echo_info "Starting with config:"
    cat config.toml
    exec env RUST_LOG=info /riffle/target/release/riffle-server --config config.toml
    ;;

  spark-client)
    # ========== Spark Client (Interactive) ==========
    echo_info "Waiting for Riffle Servers to be ready..."
    RIFFLE_SERVER_1_HOST=${RIFFLE_SERVER_1_HOST:-riffle-server-1}
    RIFFLE_SERVER_2_HOST=${RIFFLE_SERVER_2_HOST:-riffle-server-2}
    
    for i in {1..30}; do
        if curl -f http://${RIFFLE_SERVER_1_HOST}:19998/metrics >/dev/null 2>&1 && \
           curl -f http://${RIFFLE_SERVER_2_HOST}:19999/metrics >/dev/null 2>&1; then
            echo_info "Both Riffle Servers are ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            echo_error "Riffle Servers not ready after 60 seconds"
            exit 1
        fi
        echo "Waiting for Riffle Servers... ($i/30)"
        sleep 2
    done

    echo_info "==========================================="
    echo_info "Spark Client is ready. Services available:"
    echo_info "  - Uniffle Coordinator: http://${COORDINATOR_HOST:-coordinator}:19995"
    echo_info "  - Riffle Server 1: http://${RIFFLE_SERVER_1_HOST}:19998"
    echo_info "  - Riffle Server 2: http://${RIFFLE_SERVER_2_HOST}:19999"
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
    COORDINATOR_HOST=${COORDINATOR_HOST:-coordinator}
    RIFFLE_SERVER_1_HOST=${RIFFLE_SERVER_1_HOST:-riffle-server-1}
    RIFFLE_SERVER_2_HOST=${RIFFLE_SERVER_2_HOST:-riffle-server-2}
    
    for i in {1..30}; do
        if curl -f http://${RIFFLE_SERVER_1_HOST}:19998/metrics >/dev/null 2>&1 && \
           curl -f http://${RIFFLE_SERVER_2_HOST}:19999/metrics >/dev/null 2>&1; then
            echo_info "Both Riffle Servers are ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            echo_error "Riffle Servers not ready after 60 seconds"
            exit 1
        fi
        echo "Waiting for Riffle Servers... ($i/30)"
        sleep 2
    done

    # Run Spark SQL Integration Test
    echo_info "Running basic test..."
    cd ${SPARK_HOME}

    # case1: with sql_set sqls
    if ./bin/spark-shell \
        --master local[1] \
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
    if ./bin/spark-sql --master local[1] -f "$MERGED_SQL"; then
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
    echo_info "  - riffle-server-1: Start Riffle Server 1"
    echo_info "  - riffle-server-2: Start Riffle Server 2"
    echo_info "  - spark-client: Start Spark client (interactive)"
    echo_info "  - run-tests: Run full integration tests"
    exit 1
    ;;
esac
