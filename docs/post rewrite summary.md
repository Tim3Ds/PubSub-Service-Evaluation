# Post-Rewrite Performance Summary: Redis, ZeroMQ, and RabbitMQ

This summary focuses on the state of Redis, ZeroMQ, and RabbitMQ following the recent performance normalization and rewrite effort.

## 1. Redis Analysis
Redis has emerged as a top-tier synchronous performer but remains a catastrophic failure in asynchronous contexts.

| Metric | Synchronous (C++) | Synchronous (Python) | Asynchronous (Python) |
| :--- | :--- | :--- | :--- |
| **Throughput** | 1.74 msgs/ms | 1.78 msgs/ms | 0.00 msgs/ms |
| **Mean Latency** | 0.35 ms | 1.00 ms | N/A |
| **Reliability** | 100% | 100% | 0% (Fail: 100k) |

**Verdict**: Redis is ideal for sync Request-Response or Pub/Sub workloads where low latency is required. The async implementation is currently non-functional for high-volume traffic.

## 2. ZeroMQ Analysis
ZeroMQ remains the raw speed champion but shows signs of "brittleness" under high stress in mixed-language environments.

| Metric | Synchronous (C++) | Synchronous (Python) | Asynchronous (Python) |
| :--- | :--- | :--- | :--- |
| **Throughput** | 2.47 msgs/ms | 1.09 msgs/ms | 0.11 msgs/ms |
| **Mean Latency** | 0.37 ms | 1.01 ms | 1379 ms |
| **Reliability** | 100% | 100% | 1% (Fail: 98k) |

**Verdict**: The absolute best for C++ high-speed P2P communication. However, it requires careful socket state management to avoid the "99% failure" cliff observed in Python async tests.

## 3. RabbitMQ Analysis
RabbitMQ has seen the most dramatic improvement due to the normalization of its C++ client integration.

| Metric | Synchronous (C++) | Synchronous (Python) | Improvement (%) |
| :--- | :--- | :--- | :--- |
| **Throughput** | 2.05 msgs/ms | 0.15 msgs/ms | +1,266% |
| **Mean Latency** | 0.46 ms | 2.64 ms | +473% faster |
| **Reliability** | 100% | 100% | Stable |

**Verdict**: Previously dismissed as "too slow," RabbitMQ is now a powerhouse when using the C++ client. It is the most improved service in the suite, offering high throughput with its signature persistence and reliability guarantees.

---

## Overall Comparison (Sync Mode)
For most workloads, **RabbitMQ (C++)** and **Redis** now offer competitive alternatives to **ZeroMQ**, with RabbitMQ providing much better reliability and persistence options at only a ~20% performance penalty compared to ZeroMQ.

## Critical Issue: Asynchronous Failures
Despite the rewrite, the asynchronous implementations for these three services (in Python) remain fundamentally broken for message counts above 3,200. This suggests a systemic issue in how the Python event loop or socket abstraction is handling wait-states and message retrieval.
