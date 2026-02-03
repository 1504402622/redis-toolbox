# redis-toolbox

## 使用方式

生成默认配置（TOML）：

```
redis-toolbox init ./sync.toml
```

使用配置运行：

```
redis-toolbox ./sync.toml
```

如果不带参数，会自动加载当前目录下的 `sync.toml`（存在时）。

## 运行说明

- 当前仅支持 `mode = "sync"`。
- 如果要接入真实 Redis，需替换 `ReaderClient` 的 `CommandSource` 实现，读取 PSYNC/RDB/AOF。

## 核心流程

1. 加载 TOML 配置并初始化 client/log/filter。
2. 创建 reader/writer 客户端，维护 offset/replid 与各自的 channel。
3. 启动同步：全局 channel 缓冲、过滤器筛选、writer 批量 pipeline 写入。
4. 异常容灾：global channel 溢出丢弃 ，reader\writer 断线重连。


