
---

# Stale Read 检测机制说明

本机制用于检测交易执行过程中的陈旧读（Stale Read）并自动回滚污染的读写集，提升并发执行的正确性。

---

## 开关控制方式

通过环境变量控制是否启用：

### ✅ 启用：

```bash
export STALE_READ_PROTECTION_ENABLED=true
```

### ❌ 禁用（默认）：

```bash
export STALE_READ_PROTECTION_ENABLED=false
# 或不设置该变量
```

---

## 生效范围

启用后，将影响以下模块行为：

* 检测读到已被其他交易写入的 key（方法：ApplyTxSimContext(...))；
* 拒绝提交发生陈旧读的交易（方法：handletx()）；
* 回滚受污染的读写集（方法：handleTxRequest(...) 中调用 rollbackRWSet(...)）；
* 重新执行交易以获取正确数据。


如需在代码中判断该机制是否启用，请使用：
//
```go
import "chainmaker.org/chainmaker/module/core/common/stalecontrol"

if stalecontrol.IsEnabled() {
    // 执行相关逻辑
}
```

---

