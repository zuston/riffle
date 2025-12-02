#!/bin/bash
set -e

# Source environment variables
source ~/.bashrc

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Create necessary directories
echo_info "Creating necessary directories..."
mkdir -p /tmp/riffle-server-1/data
mkdir -p /tmp/riffle-server-2/data
mkdir -p ${UNIFFLE_HOME}/logs

# Start Uniffle Coordinator
echo_info "Starting Uniffle Coordinator..."
cd ${UNIFFLE_HOME}

# Check if coordinator is already running
if nc -z localhost 21000 2>/dev/null; then
    echo_warn "Coordinator already running on port 21000"
else
    echo_info "Starting coordinator..."
    nohup ./bin/start-coordinator.sh > logs/coordinator.log 2>&1 &
    COORDINATOR_PID=$!
    echo $COORDINATOR_PID > /tmp/uniffle-coordinator.pid
    echo_info "Coordinator started with PID: $COORDINATOR_PID"

    # Wait for coordinator to be ready
    echo_info "Waiting for coordinator to be ready..."
    for i in {1..30}; do
        if curl -f http://localhost:19995/api/app/total 2>/dev/null || nc -z localhost 21000 2>/dev/null; then
            echo_info "Coordinator is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            echo_error "Coordinator failed to start within timeout"
            echo_error "=== Coordinator logs (last 20 lines) ==="
            tail -20 logs/coordinator.log 2>/dev/null || echo "No log file found"
            exit 1
        fi
        echo "Waiting for coordinator... ($i/30)"
        sleep 2
    done
fi

# Build Riffle Server if not exists
if [ ! -f /riffle/target/release/riffle-server ]; then
    echo_info "Building Riffle Server..."
    cd /riffle
    cargo build --release
fi

WORKER_IP=127.0.0.1

# Start Riffle Server 1
echo_info "Starting Riffle Server 1..."
cd /tmp/riffle-server-1
cp ${RIFFLE_HOME}/conf/riffle.conf.1 config.toml
RUST_LOG=info nohup /riffle/target/release/riffle-server --config config.toml > server1.log 2>&1 &
RIFFLE_SERVER_1_PID=$!
echo $RIFFLE_SERVER_1_PID > /tmp/riffle-server-1.pid
echo_info "Riffle Server 1 started with PID: $RIFFLE_SERVER_1_PID"
sleep 5

# Start Riffle Server 2
echo_info "Starting Riffle Server 2..."
cd /tmp/riffle-server-2
cp ${RIFFLE_HOME}/conf/riffle.conf.2 config.toml
RUST_LOG=info nohup /riffle/target/release/riffle-server --config config.toml > server2.log 2>&1 &
RIFFLE_SERVER_2_PID=$!
echo $RIFFLE_SERVER_2_PID > /tmp/riffle-server-2.pid
echo_info "Riffle Server 2 started with PID: $RIFFLE_SERVER_2_PID"
sleep 5

# Verify Riffle Servers are running
echo_info "Verifying Riffle Servers..."
sleep 3
if curl -f http://localhost:19998/metrics >/dev/null 2>&1; then
    echo_info "Riffle Server 1 is running"
else
    echo_warn "Riffle Server 1 metrics not ready"
    echo_warn "=== Riffle Server 1 log (last 40 lines) ==="
    tail -40 /tmp/riffle-server-1/server1.log 2>/dev/null || echo_warn "No log file found"
    exit 1
fi

if curl -f http://localhost:19999/metrics >/dev/null 2>&1; then
    echo_info "Riffle Server 2 is running"
else
    echo_warn "Riffle Server 2 metrics not ready"
fi

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
