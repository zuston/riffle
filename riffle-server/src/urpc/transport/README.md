# urpc Transport 层

这个模块提供了 urpc 的网络传输抽象，支持多种 I/O 后端：

1. **epoll** - 使用 tokio 的 TcpStream（默认，跨平台）
2. **io-uring** - 使用 Linux io-uring（仅 Linux，需要启用 feature）

## 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                     urpc Server/Client                       │
├─────────────────────────────────────────────────────────────┤
│                    Connection<S: TransportStream>            │
├─────────────────────────────────────────────────────────────┤
│  Transport trait          TransportListener trait           │
│       │                           │                         │
│       ▼                           ▼                         │
│  ┌─────────┐                 ┌──────────┐                  │
│  │ Epoll   │                 │ Uring    │                  │
│  │Transport│                 │Transport │                  │
│  └────┬────┘                 └────┬─────┘                  │
│       │                           │                         │
│       ▼                           ▼                         │
│  ┌─────────┐                 ┌──────────┐                  │
│  │ Epoll   │                 │ Uring    │                  │
│  │Listener │                 │Listener  │                  │
│  └────┬────┘                 └────┬─────┘                  │
│       │                           │                         │
│       ▼                           ▼                         │
│  ┌─────────┐                 ┌──────────┐                  │
│  │ Epoll   │                 │ Uring    │                  │
│  │ Stream  │                 │ Stream   │                  │
│  └─────────┘                 └──────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

## 使用方法

### 1. 默认使用 epoll（无需配置）

```toml
[urpc_config]
# 默认使用 epoll，无需额外配置
get_index_rpc_version = "V2"
```

### 2. 启用 io-uring（Linux only）

在 `Cargo.toml` 中启用 feature：

```toml
[features]
io-uring = ["dep:io-uring"]
```

在配置文件中启用：

```toml
[urpc_config]
get_index_rpc_version = "V2"
io_uring_enable = true
io_uring_threads = 2  # 可选，默认为 2
```

## 配置选项

### UrpcConfig

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `get_index_rpc_version` | RpcVersion | V1 | RPC 版本 |
| `io_uring_enable` | bool | false | 是否启用 io-uring（仅 Linux） |
| `io_uring_threads` | usize | 2 | io-uring 工作线程数 |

## 性能对比

| 场景 | epoll | io-uring |
|------|-------|----------|
| 小数据量读写 | 良好 | 良好 |
| 大数据量传输 | 良好 | 优秀（减少系统调用） |
| 高并发连接 | 良好 | 优秀（批处理） |
| CPU 使用率 | 中等 | 低 |

## 注意事项

1. **平台支持**：io-uring 仅在 Linux 5.1+ 上可用
2. **内核版本**：建议 Linux 5.10+ 以获得最佳性能
3. **线程模型**：io-uring 使用独立的工作线程池处理 I/O
4. **零拷贝**：io-uring transport 支持通过 sendfile/splice 进行零拷贝传输

## 实现细节

### Epoll Transport

- 基于 tokio::net::TcpStream
- 使用标准的 async/await 模式
- 跨平台支持（Linux/macOS/Windows）

### Io-uring Transport

- 基于 io-uring crate 的底层绑定
- 使用独立的 io-uring 实例处理网络 I/O
- 使用 SQE/CQE 进行异步操作提交/完成
- 支持的操作：Accept, Connect, Recv, Send, Close
