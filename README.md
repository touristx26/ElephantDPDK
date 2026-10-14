# ElephantDPDK —— QLF 分支(feature/qlf)

**Ring 打满时间(TTF, time-to-full)预测实验框架**。本分支研究:DPDK pipeline 中 `dispatcher -> worker` 的 rte_ring 何时会被打爆,并在**受控实验场景**下对比多种预测算法的性能。

> 大象流检测版本(NitroSketch + 时间窗口)在 `main` 分支。

## 实验架构

```
                 RX lcore (producer)                    monitor lcore (独立监视线程)
     合成流量发生器(令牌桶限速)                      ┌────────────────────────────┐
     rate(t) = gen_pps + ramp·t          │  每 sample_us(默认 100us, TSC 忙等):  │
        │                                  │   1. 采样 depth + enq/deq 计数器快照  │
        ▼                                  │   2. 相邻差值 -> arrival/service 率  │
     flow hash → dispatcher ──── drops ──▶ │   3. 运行全部预测算法               │
        │           │                      │   4. depth/arrival/service 三指标   │
        ▼           ▼                      │      满载风险判断(限频 ALERT)       │
   worker_ring[0] worker_ring[N]   │   5. drop 上升沿 = 真实溢出,          │
        │           │                      │      结算 trial 误差 + CSV 记录      │
        ▼           ▼                      └────────────────────────────┘
   Worker lcore (consumer, 模拟 DPI)
   每包: payload 模式扫描(memchr)          main lcore
       + 可配置忙等(--dpi-us)           ┌────────────────────────────┐
   消费能力 ≈ 1/dpi_us 包/秒/worker     │  周期状态打印 / 退出时汇总报告   │
   (dispatcher 维护 per-ring 单调       └────────────────────────────┘
    enq/deq 计数器, 永不清零)
```

**打爆的条件**:生产速率 > 总消费能力时,ring 占用持续上升,深度达到 4096 后 dispatcher 开始丢包(drop 计数器增长)。预测算法的任务是在上升阶段估计"还剩多久打满"。

## 四种预测算法

| 算法 | 输入 | 原理 | 特点 |
|---|---|---|---|
| `rate` | enq/deq 计数器差值 | `TTF = (capacity-depth) / (arrival−service)`,本周期速率差直接外推 | **当前阶段核心**;响应最快,单周期突发噪声敏感 |
| `linreg` | depth 序列 | 最小二乘拟合最近 W 个样本的斜率,`TTF = 剩余空间/斜率` | 稳定,对噪声不敏感,但对趋势变化响应慢 |
| `ewma` | depth 序列 | 相邻样本间填充速率的指数加权平均(α=0.3),`TTF = 剩余空间/平滑速率` | 响应快,易受采样噪声影响 |
| `holt` | depth 序列 | Holt 双指数平滑(水平 α=0.5 + 趋势 β=0.25),`TTF = (容量-水平)/趋势` | 兼顾水平与趋势,对变负载适应性较好 |

仅当存在上升趋势时给出有效预测;占用率下降/平稳/ring 已满时返回无效。

**满载风险告警**(depth / arrival / service 三指标):`depth ≥ 80% 容量 && growth>0 && ttf_rate ≤ 500ms` 时打印 `ALERT`(限频 200ms),状态行同步显示 `RISK` 标志。

## 预测性能指标(每个溢出 trial 结算一次)

- **err** = (预测发出时刻 + TTF) − 真实溢出时刻(正值 = 预测偏晚/晚报)
- **MAE**:全部有效预测的平均绝对误差
- **near MAE**:仅统计**溢出前 2 秒内**发出的预测的 MAE(近端精度,预警场景最关心)
- **bias**:平均带符号误差(系统性早报/晚报倾向)
- **first lead**:最早一条有效预测距溢出的提前量(告警最多能提前多久)

一个 "trial" = 从上一个溢出事件(或启动)到下一次溢出的完整填充过程。

## 目录结构

```
meson.build
src/
├── elephant_common.h        # flow key/hash、mbuf dynfield 标记、常量
├── app_ctx.h/.c             # 实验参数配置、force_quit
├── rx/rx.h/.c               # 受控合成流量(令牌桶: 初始 pps+爬升+锯齿)
├── dispatcher/dispatcher.h/.c # flow hash → ring 分发(含 per-ring drop 计数)
├── worker/worker.h/.c        # 模拟 DPI 消费(memchr 扫描 + 忙等)
├── monitor/ringmon.h/.c      # ring 占用采样、溢出 trial 检测、指标统计
├── predict/predict.h/.c      # linreg / ewma / holt 三种 TTF 预测算法
└── main.c                   # 实验主框架: 参数、lcore 编排、主循环、报告
```

## 编译

前置条件:已构建的 DPDK(20.11 ~ 最新)及其 `libdpdk.pc`,meson + ninja。

```bash
export PKG_CONFIG_PATH=~/dpdk/build/meson-uninstalled:$PKG_CONFIG_PATH
cd ElephantDPDK
git checkout feature/qlf
meson setup build
ninja -C build
```

## 运行实验

### 默认实验(无网卡,~11 秒后第一次溢出)

```bash
# lcore 布局: main + 1 RX + 2 worker + 1 monitor = 5 个核
sudo ./build/elephantdpdk -l 0-4 -n 4 --no-pci -- -s
```

默认参数:生产 100K pps 起步、以 10K pps/s 爬升;2 个 worker、每个 `--dpi-us 10`(总消费能力 ≈ 200K pps)。约 10 秒时生产越过消费能力,再约 1 秒填满 4096 深的 ring → **约 11 秒时观察到第一次溢出**,随后每个 trial 打印一行算法误差,Ctrl-C 退出输出汇总表。

### 参数说明

命令行分两段:`elephantdpdk [EAL 选项] -- [应用选项]`。`-l`/`-n`/`-a`/`--no-pci` 等由 DPDK EAL 解析,`--` 之后才是本实验参数。

lcore 需求:**1(main)+ n_rx + n_workers + 1(monitor)**,独立 monitor lcore 固定占一个核(见"已知限制")。

#### 速查表

| 参数 | 说明 | 取值范围 | 默认 |
|---|---|---|---|
| `-q, --rx-queues <n>` | RX 队列数(= RX lcore 数) | 1..8 | 1 |
| `-w, --workers <n>` | worker 数(= ring 数) | 1..16 | 2 |
| `-s, --synthetic` | 软件合成流量(无需网卡) | - | off |
| `--dpi-us <us>` | worker 每包模拟 DPI 忙等 | 0..10000 | 10 |
| `--gen-pps <pps>` | 合成流量初始速率(每 RX lcore) | ≥0 | 100000 |
| `--gen-ramp <pps/s>` | 速率线性爬升斜率 | ≥0 | 10000 |
| `--gen-sawtooth <s>` | 周期重置爬升,产生多个 trial | ≥0 | 0 |
| `--sample-us <us>` | monitor 采样周期 | 1..1000000 | 100 |
| `--pred-window <n>` | depth 序列拟合窗口(采样数) | 4..16384 | 512 |
| `--csv <path>` | 全量数据 CSV 输出 | - | 无 |
| `-i, --stats-period <s>` | 运行状态行打印间隔(秒) | ≥1 | 2 |
| `-p, --port <id>` | NIC 端口(NIC 模式) | - | 0 |

#### 生产侧(合成流量模型)

- **`-s, --synthetic`**:启用软件令牌桶流量发生器,无需网卡(配合 `--no-pci`)。每 RX lcore 一个独立发生器,速率模型 `rate(t) = gen_pps + gen_ramp × elapsed`;流亲和保留,流量由 64 条合成流构成(4 条 1400B 重流 / 12 条 512B 中流 / 48 条 64B 鼠流,按 25%/25%/50% 加权混发)。不开启此模式则必须配置真实 NIC 端口。

- **`--gen-pps <pps>`**:每 RX lcore 的初始速率(令牌/秒),总生产速率 = `gen_pps × n_rx`。设为 `0` 且无 ramp/sawtooth 时为**全速压力模式**,速率仅受 mempool 分配上限约束。起点若已高于总消费能力,ring 从一开始就持续填充。

- **`--gen-ramp <pps/s>`**:速率随时间线性爬升。**穿越消费能力的时刻** `t_cross = (总消费能力 − gen_pps) / gen_ramp`,其中总消费能力 ≈ `n_workers / dpi_us` pps。想控制"多少秒后开始涨满",主要调它。

- **`--gen-sawtooth <s>`**:每 s 秒将 elapsed 清零,速率回落到 gen_pps 重新爬升,形成"填充 → 溢出 → 速率回落 → 排空 → 再填充"循环,**单次运行产生多个 trial**,统计更稳。`0` = 单调爬升,溢出后持续过载,不再产生新 trial。

- **`-q, --rx-queues <n>`**:RX 队列数,每队列绑一个 RX lcore。>1 时 dispatcher ring 从 SP/SC 退化为 MP/MC(仍无锁,但略慢);NIC 模式还要求端口支持 RSS,synthetic 模式下总生产速率随队列数线性放大。

#### 消费侧(worker)

- **`--dpi-us <us>`**:worker 每包的确定性忙等微秒数(外加一次 payload memchr 扫描)。单 worker 消费能力 ≈ `1/dpi_us` pps,**总消费能力 ≈ `n_workers / dpi_us`**(默认 2/10µs = 200K pps)。`0` = 只扫描不忙等,消费能力由访存决定(极高)。它与 `--gen-pps/--gen-ramp` 构成受控实验的生产/消费两端。

- **`-w, --workers <n>`**:worker 数,每 worker 一条独立 4096 深的 ring,flow hash 保证同流同 worker(2 的幂用位掩码分发,否则取模)。**每条 ring 独立检测溢出、独立结算 trial**,worker 数即 trial 并行度。

#### 监视与预测(monitor lcore)

- **`--sample-us <us>`**:monitor 的采样周期(TSC 忙等保证精确边界)。同时决定三件事:① ground truth 的时间量化误差 ≤ 一个周期;② `rate` 算法的差分窗口(越小响应越快、越噪);③ 历史/预测日志的覆盖时长 = `16384 × sample_us`(100µs ≈ 1.6s,1ms ≈ 16.4s)。放大采样周期前先确认 trial 的填充时长仍在覆盖范围内。无论周期多大,monitor 均忙等占满一个核。

- **`--pred-window <n>`**:linreg/ewma/holt 的拟合窗口(采样点数),100µs 采样下 512 ≈ 51ms。窗口大 → 平滑强、对趋势变化响应慢;窗口小 → 噪声敏感。`rate` 算法不受此参数影响(它只用当前一个周期的计数器差分)。

- **`--csv <path>`**:每采样周期每 ring 写一行:`t_ms, ring, depth, depth_pct, enq_pkts, deq_pkts(累计计数器), arrival_pps, service_pps, growth_pps, ttf_rate, ttf_linreg, ttf_ewma, ttf_holt`(无有效预测时对应 TTF 记 -1)。由 monitor lcore 直接写入,glibc FILE 内部锁保证线程安全。

#### 其他

- **`-i, --stats-period <s>`**:main lcore 运行状态行的打印间隔(秒),只影响显示,不影响实验与测量。
- **`-p, --port <id>`**:NIC 模式端口 id;promiscuous 强制开启,RSS offload 按端口实际支持项过滤。

### 典型实验场景

```bash
# 场景 1: 单次缓慢填充(观察预测随趋势收敛的过程)
sudo ./build/elephantdpdk -l 0-4 -n 4 --no-pci -- -s \
    --gen-pps 50000 --gen-ramp 5000 --dpi-us 10 --csv run1.csv

# 场景 2: 锯齿负载, 单次运行 3 次溢出 trial(30s 周期, 更稳的统计)
sudo ./build/elephantdpdk -l 0-4 -n 4 --no-pci -- -s \
    --gen-pps 50000 --gen-ramp 20000 --gen-sawtooth 30 --dpi-us 10 --csv run2.csv

# 场景 3: 突发打满(观察各算法对陡峭趋势的响应; 全速模式采样调密)
sudo ./build/elephantdpdk -l 0-4 -n 4 --no-pci -- -s \
    --gen-pps 0 --dpi-us 20 --sample-us 10

# 场景 4: 真实网卡流量(外部打流设备控制负载)
sudo ./build/elephantdpdk -l 0-7 -n 4 -a 0000:3b:00.0 -- -p 0 -q 2 -w 4 --dpi-us 10
```

### 输出示例

运行中(每 2s,含三指标与风险标志):

```
---- gen 0.15 Mpps | ring0[42% arr0.07M svc0.06M grow+0.01M] ring1[41% arr0.08M svc0.06M grow+0.02M] | ttf: rate=8420ms linreg=8500ms ewma=8710ms holt=8500ms | drops 0 ----
```

风险告警(monitor lcore, 限频 200ms):

```
[ring 0] ALERT: depth=3400/4096 (82%), arrival=210000 pps, service=196000 pps, growth=+14000 pps, ttf=49 ms
```

溢出 trial 结算:

```
[ring 0] OVERFLOW trial #1 @ 11034 ms: rate[n=31200 MAE=180ms nearMAE=95ms bias=-32ms lead=9100ms] linreg[n=31200 MAE=812ms ...] ewma[...] holt[...]
```

Ctrl-C 后汇总:

```
============ QLF 实验最终报告 ============
生产: 1533210 pkts | 消费: 1529980 pkts | drops: 3230
ring 0: 溢出 trial 1 次 ...
算法性能汇总(共 2 个 trial):
algo      n_pred    MAE(ms)   bias(ms)     nearMAE(ms)  first_lead(ms)
rate        62400      162.5      -28.3           88.7         9100.0
linreg      59800      742.3      -85.1          121.4         9100.0
ewma        57200      911.7      -43.9          165.8         9340.0
holt        58900      703.6      -71.2          108.9         9210.0
===========================================
CSV 已写入 run1.csv
```

## 扩展新算法

在 [predict.h](src/predict/predict.h) 中增加算法枚举与声明,在 [predict.c](src/predict/predict.c) 实现 `pred_xxx(t_ms, depth, n, capacity, &ttf)` 统一签名,并在 [ringmon.c](src/monitor/ringmon.c) 的采样函数中追加一次调用即可,指标统计与 CSV 全部自动覆盖。基于计数器速率差的算法(如 `rate`)也可像它一样直接在 ringmon 内实现。

## 已知限制(demo 范围)

- ground truth 的时刻量化误差为一个采样间隔(默认 100µs)。
- 溢出后持续过载期间,占用率停在满位、预测无效,直至负载回落后重新填充。
- 全速压力模式(`--gen-pps 0`)下打满极快(毫秒级),采样间隔需调小才有意义。
- worker 的 memchr 扫描 + 忙等是消费能力的近似;真实 DPI 负载特征(缓存、分支、TLB)未建模。
- 100µs 采样下 monitor lcore 为 TSC 忙等(占满一个核),这是换取采样精度的代价。
- 预测日志/depth 历史为 16384 条循环缓冲,100µs 采样下 trial 结算覆盖溢出前约 1.6s 的预测。
