# Riffle Integration Testing with Docker Compose

This directory contains Docker Compose configuration for Riffle integration testing, with services split into independent roles.

## Architecture

Services are split into the following roles:

1. **uniffle-coordinator**: Uniffle coordinator for task assignment and scheduling
2. **riffle-server-1**: First Riffle Shuffle server
3. **riffle-server-2**: Second Riffle Shuffle server
4. **spark-client**: Spark client for running queries and tests
5. **riffle-test**: Complete test service (includes all test cases)

All services are connected via the `riffle-network` network, supporting hostname-based communication.

## Quick Start

### Start all services

```bash
cd dev/integration
docker-compose up -d
```

This starts coordinator, two riffle-servers, and spark-client.

### Check service status

```bash
docker-compose ps
```

### View logs

```bash
# View all service logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f uniffle-coordinator
docker-compose logs -f riffle-server-1
docker-compose logs -f riffle-server-2
docker-compose logs -f spark-client
```

### Access Spark Client

```bash
docker-compose exec spark-client /bin/bash
```

Then run Spark commands:

```bash
# Start Spark Shell
spark-shell --master local[*]

# Start Spark SQL
spark-sql --master local[*]
```

### Run tests

```bash
docker-compose --profile test up riffle-test
```

### Stop all services

```bash
docker-compose down
```

## Web UI Access

- **Uniffle Coordinator**: http://localhost:19995
- **Riffle Server 1 Metrics**: http://localhost:19998/metrics
- **Riffle Server 2 Metrics**: http://localhost:19999/metrics
- **Spark UI**: http://localhost:4040 (when Spark jobs are running)

## Port Mapping

| Service | Port | Description |
|---------|------|-------------|
| uniffle-coordinator | 19995 | Web UI |
| uniffle-coordinator | 21000 | RPC |
| riffle-server-1 | 19998 | HTTP/Metrics |
| riffle-server-1 | 21100 | gRPC |
| riffle-server-2 | 19999 | HTTP/Metrics |
| riffle-server-2 | 21101 | gRPC |
| spark-client | 4040 | Spark UI |

## Service Startup Order

Services start in the following order to ensure correct dependencies:

1. **uniffle-coordinator** starts first
2. **riffle-server-1** and **riffle-server-2** wait for coordinator health check
3. **spark-client** waits for both riffle-servers to be healthy

Each service has `healthcheck` configured to ensure readiness.

## Partial Startup

### Start only Coordinator

```bash
docker-compose up -d uniffle-coordinator
```

### Start Coordinator and Riffle Servers

```bash
docker-compose up -d uniffle-coordinator riffle-server-1 riffle-server-2
```

## Configuration

### Environment Variables

In `spark-client` and `riffle-test` services, configure service addresses via environment variables:

- `COORDINATOR_HOST`: Coordinator hostname (default: coordinator)
- `RIFFLE_SERVER_1_HOST`: Riffle Server 1 hostname (default: riffle-server-1)
- `RIFFLE_SERVER_2_HOST`: Riffle Server 2 hostname (default: riffle-server-2)

### Configuration Files

- **coordinator.conf**: Uniffle Coordinator configuration
- **riffle.conf.1**: Riffle Server 1 configuration
- **riffle.conf.2**: Riffle Server 2 configuration
- **spark-defaults.conf**: Spark default configuration (uses Riffle as Shuffle Manager)

## Troubleshooting

### Coordinator fails to start

```bash
docker-compose logs uniffle-coordinator
```

Check if ports are occupied:
```bash
lsof -i :19995
lsof -i :21000
```

### Riffle Server cannot connect to Coordinator

Check network connectivity:
```bash
docker-compose exec riffle-server-1 nc -zv coordinator 21000
```

### Spark tests fail

Enter spark-client container to view detailed logs:
```bash
docker-compose exec spark-client /bin/bash
ls -la /opt/spark/logs/
```

## Development

### Hot reload Riffle Server

After modifying Riffle code, rebuild and restart services:

```bash
# Restart riffle-server
docker-compose restart riffle-server-1 riffle-server-2

# Or force rebuild
docker-compose up -d --build riffle-server-1 riffle-server-2
```

### Local development mode

With local code directory mounted, compile inside container:

```bash
docker-compose exec riffle-server-1 /bin/bash
cd /riffle
cargo build --release
# Then exit container and restart service
```

### Clean up

```bash
# Stop and remove containers, networks
docker-compose down

# Remove volumes (if persistent data exists)
docker-compose down -v

# Rebuild images
docker-compose build --no-cache
```

## Performance Tuning

### Adjust Riffle Server configuration

Edit `riffle.conf.1` or `riffle.conf.2`:

```toml
[memory_store]
capacity = "4G"  # Increase memory capacity

[runtime_config]
read_thread_num = 40   # Increase read threads
write_thread_num = 100 # Increase write threads
```

Then restart services:
```bash
docker-compose restart riffle-server-1 riffle-server-2
```

### Adjust Spark configuration

Edit `spark-defaults.conf`:

```properties
spark.executor.instances 4
spark.executor.cores 4
spark.executor.memory 4g
spark.sql.shuffle.partitions 8
```
