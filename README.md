# ElephantDPDK

基于 DPDK 的高性能**大象流检测与智能数据包分发系统**(demo)。

- 多 RXQ → 多 Ring/Worker 架构,Flow Hash 分发(同流亲和,保序)
- 每包提取 Flow Key/Hash 与 packet length,更新 **NitroSketch** 检测大象流
- 检测结果通过 `rte_mbuf` 元数据打标(`rte_mbuf_dynfield` 注册的 64-bit 字段,`udata64` 语义),下游零成本读取
- Fast Path 无全局锁:per-lcore sketch + 无锁 `rte_ring`(单 RX lcore 时自动升级为 SP/SC)
- Meson 构建,直接链接现有 DPDK build 产出的 `libdpdk.pc`
- 自带 `--synthetic` 软件流量发生器,无网卡也能跑通全流程

## 架构与数据路径

```
                     +-------------------------------------------------+
                     |                RX lcore (per RXQ)               |
                     |                                                 |
  NIC RXQ 0 ---->    |  rte_eth_rx_burst()  (或 synthetic 生成器)       |
  NIC RXQ 1 ---->    |        |                                        |
       ...           |        v                                        |
                     |  Flow Key 提取 (Eth/VLAN/IPv4/IPv6 + TCP/UDP)   |
                     |        |                                        |
                     |        v                                        |
                     |  63-bit Flow Hash (MurmurHash3 fmix64)          |
                     |        |                                        |
                     |        v                                        |
                     |  NitroSketch update + estimate (per-lcore, 无锁) |
                     |        |                                        |
                     |        v  est >= threshold ?                    |
                     |  Elephant Flag -> mbuf dynfield (bit63=flag,     |
                     |        |             bit62..0 = flow hash)      |
                     +--------|----------------------------------------+
                              v
                   Hash Dispatcher (按 flow hash 选 ring,
                    同流必进同一 worker;按 worker 批量入队)
                              |
            +-----------------+------------------+
            v                 v                  v
      worker_ring[0]    worker_ring[1]  ...  worker_ring[N]
            |                 |                  |
            v                 v                  v
      Worker lcore 0    Worker lcore 1  ...  Worker lcore N
      (dequeue burst, 读取 dynfield 标记, 分类统计, 释放 mbuf;
       真实业务在此分流:慢路径 / 专用 TX 队列 / 抓包等)
```

数据路径(fast path,每包顺序):

```
RXQ -> Flow Key/Hash 提取 -> NitroSketch 更新/查询 -> Elephant Flag (dynfield)
    -> Hash Dispatcher -> Ring -> Worker
```

## 目录结构

```
meson.build                    # Meson 构建,链接 libdpdk.pc
src/
├── elephant_common.h          # Flow Key / 63-bit hash / dynfield 标记布局 / 常量
├── app_ctx.h/.c               # 运行时配置、force_quit 标志
├── rx/rx.h/.c                 # RX fast path:收包/合成流量、Flow 提取、打标、分发
├── nitrosketch/
│   └── nitrosketch.h/.c       # NitroSketch(per-lcore、无锁,geometric 采样)
├── dispatcher/
│   └── dispatcher.h/.c        # Hash -> Ring 分发(SP/SC 或 MP/MC rte_ring)
├── worker/worker.h/.c         # Ring 消费者:读标记、分类统计、释放 mbuf
└── main.c                     # EAL 初始化、参数解析、端口初始化、lcore 编排
```

## 设计要点

| 主题 | 实现 |
|---|---|
| 大象流判定 | 每包把 `pkt_len` 记入本 lcore 私有的 NitroSketch(4 行 × 1024 列,采样率 0.9^j 的 geometric-interarrival 采样),随后查询最小方差估计,`est >= threshold`(默认 1 MiB)即打标。可用 `-W` 升级为**双 sketch 滑动窗口**判定(见下文) |
| **双 sketch 滑动窗口**(`-W`) | 每个 RX lcore 持有 2 个 sketch(共 8 行):当前窗口 `sketch[epoch&1]` 与上一窗口 `sketch[(epoch&1)^1]`。主 lcore 每 `stats_period_s` 秒广播一次 epoch 原子递增,各 RX lcore 在 burst 边界检测到变化后清零退役 sketch 并交换角色。每包对当前 sketch 记账后查询 `当前 est + 上一窗口 est`,超过 `-t` 阈值即打标。语义从"累计体积"变为"最近一个窗口内的体积"。低速率长流(如 keepalive)不再被误判,窗口过完自动衰减 |
| 数据包标记 | 传统 `mbuf->udata64` 字段已在 DPDK 20.11 中被移除,故用 `rte_mbuf_dynfield` 注册等价的 64-bit 字段(EAL init 后、mempool 使用前注册):bit63 = elephant 标志,低 63 位 = flow hash。Worker 端只需一次位运算即可分流 |
| 分发 | `dispatcher` 按 flow hash 高 32 位映射到 worker ring;先按 worker 在栈上分桶,再每 worker 一次 `rte_ring_enqueue_burst`,ring 只被打一次 |
| 无锁 | sketch 为 per-lcore 私有(无共享写);ring 为无锁 DPDK ring:单 RX lcore 时 `RING_F_SP_ENQ\|RING_F_SC_DEQ`,多 RX lcore 时 MP/MC(CAS);统计为 per-lcore 计数器,main lcore 只读聚合 |
| 背压 | ring 满时丢包并计数(`dispatcher drops`),丢弃路径为冷路径,不污染 fast path |
| 合成流量 | `--synthetic`:64 条流(4 条 1400B 重流 / 12 条 512B 中流 / 48 条 64B 鼠流,流量占比 25%/25%/50%),重流会在几秒内越过阈值,方便观察"流被判定为大象"的日志与统计变化 |

## 编译

前置条件:已构建好的 DPDK(20.11 LTS ~ 最新,代码对 21.11 前后的 ethdev 常量命名 `ETH_*`/`RTE_ETH_*` 做了版本自适应)及其 `libdpdk.pc`,以及 meson + ninja。

```bash
# 指向现有 DPDK build 目录(meson-uninstalled 下有 libdpdk.pc)
export PKG_CONFIG_PATH=~/dpdk/build/meson-uninstalled:$PKG_CONFIG_PATH
# 若 DPDK 已 make install,确保 /usr/local/lib64/pkgconfig 等在默认搜索路径即可

cd ElephantDPDK
meson setup build
ninja -C build
```

> 如果 pkg-config 找不到 libdpdk,可用 `pkg-config --variable=pcfilepath libdpdk` 确认路径;或直接 `PKG_CONFIG_PATH=<dpdk build>/meson-uninstalled`。
>
> 若编译时仍出现 API 不兼容报错,请用 `pkg-config --modversion libdpdk` 确认 DPDK 版本:本项目按 20.11+(即 `rte_` 前缀的结构体命名)~ 最新版本适配。

## 运行

### 1. 无网卡 demo(synthetic 模式)

不需要任何 PCI 设备,`--no-pci` 即可运行。至少需要 1 个 RX lcore + 2 个 worker + 1 个 main lcore:

```bash
sudo ./build/elephantdpdk -l 0-2 -n 4 --no-pci -- -s -w 2
# 观察 rx q0: ELEPHANT flow hash=0x... 日志与周期统计中的 elephant 比例上升
```

多 RXQ/多 worker(4 RX + 4 worker,即 9 个 lcore):

```bash
sudo ./build/elephantdpdk -l 0-8 -n 4 --no-pci -- -s -q 2 -w 4
```

### 2. 真实网卡模式

```bash
# 2 个 RXQ(RSS 分流)+ 4 个 worker,端口 0
sudo ./build/elephantdpdk -l 0-6 -n 4 -a 0000:3b:00.0 -- -p 0 -q 2 -w 4 -t 1048576
```

NIC 模式下多 RXQ 依赖网卡 RSS(IP/TCP/UDP)把不同流散到不同队列;若网卡不支持 RSS,流会集中在 queue 0。

### 命令行参数

| 参数 | 说明 | 默认 |
|---|---|---|
| `-p, --port <id>` | 使用的网卡端口 | 0 |
| `-q, --rx-queues <n>` | RX 队列数(= RX lcore 数) | 1 |
| `-w, --workers <n>` | worker 数(= ring 数) | 2 |
| `-t, --threshold <bytes>` | 大象流字节阈值 | 1 MiB |
| `-s, --synthetic` | 软件合成流量(无需网卡) | off |
| `-W, --time-window` | **时间窗口型判定**(双 sketch 轮转,见下文) | off |
| `-i, --stats-period <s>` | 统计打印间隔(秒)。`-W` 模式下同时作为窗口长度 | 2 |

EAL 参数(`-l`、`-n`、`-a` 等)与 DPDK 标准用法一致,`--` 之后是应用参数。

### 示例输出

```
rx q0: ELEPHANT flow hash=0x9a3f...c2 est=1065120 bytes (threshold=1048576)
---- RX: 5183232 pkts (12.3 Mpps, 321.5 MB) | elephant: 129833 pkts (2.51%) in 4 flow(s) | ...
```

## 已知限制(demo 范围)

- 大象流判定是**每 RX lcore 独立**的:同一流被 RSS 分到多个队列时,各 sketch 分别计数;跨 lcore 聚合可按需扩展(per-flow rebalance 或全局 sketch 分片)。
- IPv6 扩展头不解析,带扩展头的 IPv6 报文取不到端口(仍可跟踪五元组中可用部分)。
- 真实部署建议按 NUMA 亲和分配 mempool/ring(lcore socket),demo 简化为单 socket。
- per-packet 元数据使用 `rte_mbuf_dynfield`(与其它同样使用 dynfield 的库自动协商空间,不会互相冲突)。

## 参考

- NitroSketch: <https://github.com/yindaz/NitroSketch> (SIGCOMM'19)
- DPDK: <https://www.dpdk.org/> / <https://doc.dpdk.org/>
