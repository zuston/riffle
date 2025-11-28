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
    cargo build --features hdrs,memory-prof --release
fi

# Start Riffle Server 1
echo_info "Starting Riffle Server 1..."
cd /tmp/riffle-server-1
cp ${RIFFLE_HOME}/conf/riffle.conf.1 config.toml
WORKER_IP=localhost RUST_LOG=info nohup /riffle/target/release/riffle-server --config config.toml > server1.log 2>&1 &
RIFFLE_SERVER_1_PID=$!
echo $RIFFLE_SERVER_1_PID > /tmp/riffle-server-1.pid
echo_info "Riffle Server 1 started with PID: $RIFFLE_SERVER_1_PID"
sleep 5

# Start Riffle Server 2
echo_info "Starting Riffle Server 2..."
cd /tmp/riffle-server-2
cp ${RIFFLE_HOME}/conf/riffle.conf.2 config.toml
WORKER_IP=localhost RUST_LOG=info nohup /riffle/target/release/riffle-server --config config.toml > server2.log 2>&1 &
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
fi

if curl -f http://localhost:19999/metrics >/dev/null 2>&1; then
    echo_info "Riffle Server 2 is running"
else
    echo_warn "Riffle Server 2 metrics not ready"
fi

# Create Spark SQL test script
echo_info "Creating Spark SQL test script..."
cat > /tmp/test_spark_sql.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

spark = SparkSession.builder \
    .appName("RiffleIntegrationTest") \
    .config("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager") \
    .config("spark.rss.coordinator.quorum", "localhost:21000") \
    .config("spark.rss.storage.type", "MEMORY_LOCALFILE") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Create test data
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "David", 40)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Test basic SQL operations
df.createOrReplaceTempView("people")

result1 = spark.sql("SELECT * FROM people WHERE age > 30")
print("Query 1 - Filter results:")
result1.show()

result2 = spark.sql("SELECT name, age FROM people ORDER BY age DESC")
print("Query 2 - Order by results:")
result2.show()

result3 = spark.sql("SELECT COUNT(*) as total, AVG(age) as avg_age FROM people")
print("Query 3 - Aggregate results:")
result3.show()

# Test join operation (triggers shuffle)
data2 = [(1, "Engineer"), (2, "Manager"), (3, "Engineer"), (4, "Designer")]
df2 = spark.createDataFrame(data2, ["id", "role"])
df2.createOrReplaceTempView("roles")

result4 = spark.sql("""
    SELECT p.name, p.age, r.role 
    FROM people p 
    JOIN roles r ON p.id = r.id
""")
print("Query 4 - Join results:")
result4.show()

spark.stop()
print("Spark SQL integration test completed successfully!")
EOF

# Run Spark SQL Integration Test
echo_info "Running Spark SQL Integration Test..."
cd ${SPARK_HOME}

# Find py4j jar dynamically
PY4J_JAR=$(find ${SPARK_HOME}/python/lib -name "py4j-*.zip" | head -1)
if [ -n "$PY4J_JAR" ]; then
    export PYTHONPATH=${SPARK_HOME}/python:${PY4J_JAR}:${PYTHONPATH}
else
    export PYTHONPATH=${SPARK_HOME}/python:${PYTHONPATH}
fi

# Run spark-submit
if ./bin/spark-submit \
    --master local[4] \
    --conf spark.shuffle.manager=org.apache.spark.shuffle.RssShuffleManager \
    --conf spark.rss.coordinator.quorum=localhost:21000 \
    --conf spark.rss.storage.type=MEMORY_LOCALFILE \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --conf spark.sql.shuffle.partitions=4 \
    --jars jars/rss-client.jar \
    /tmp/test_spark_sql.py; then
    echo_info "Spark SQL test completed successfully!"
    TEST_RESULT=0
else
    echo_error "Spark SQL test failed!"
    TEST_RESULT=1
fi

# Show logs if test failed
if [ $TEST_RESULT -ne 0 ]; then
    echo_error "=== Riffle Server 1 Logs (last 50 lines) ==="
    tail -50 /tmp/riffle-server-1/server1.log 2>/dev/null || echo "No logs found"
    echo_error "=== Riffle Server 2 Logs (last 50 lines) ==="
    tail -50 /tmp/riffle-server-2/server2.log 2>/dev/null || echo "No logs found"
    echo_error "=== Coordinator Logs (last 50 lines) ==="
    tail -50 ${UNIFFLE_HOME}/logs/coordinator.log 2>/dev/null || echo "No logs found"
fi

# Cleanup function
cleanup() {
    echo_info "Cleaning up..."
    if [ -f /tmp/riffle-server-1.pid ]; then
        kill $(cat /tmp/riffle-server-1.pid) 2>/dev/null || true
    fi
    if [ -f /tmp/riffle-server-2.pid ]; then
        kill $(cat /tmp/riffle-server-2.pid) 2>/dev/null || true
    fi
    if [ -f /tmp/uniffle-coordinator.pid ]; then
        kill $(cat /tmp/uniffle-coordinator.pid) 2>/dev/null || true
    fi
    pkill -f riffle-server 2>/dev/null || true
}

# Trap to cleanup on exit
trap cleanup EXIT

exit $TEST_RESULT

