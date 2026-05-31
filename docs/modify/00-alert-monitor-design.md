# 盘中实时告警监控 — 设计文档

## Context

用户不再需要每日 18:00 的批量分析报告，取而代之的是盘中实时告警监控：当股票出现 **MACD 死叉 + RSI < 20 + 跌破关键均线** 时立即推送飞书告警。原有每日分析工作流需停用，仅保留告警监控。

## 需求摘要

| 维度 | 决策 |
|------|------|
| 监控市场 | A 股 + 港股 |
| 监控方式 | GitHub Actions cron 定时短任务（推荐每 20 分钟） |
| 股票列表 | 与现有 `STOCK_LIST` 共用 |
| 告警去重 | 每只股票每天最多告警一次 |
| 告警内容 | 精简版：MACD 状态、RSI 值、最新价、跌破哪些均线 |
| 数据时效 | 盘中实时行情数据 |
| 触发条件 | MACD 当日刚发生死叉 AND RSI_6 < 20 AND (价格跌破 MA20/60/120/250 中至少一条) |
| 推送渠道 | 飞书 Webhook（复用现有 FeishuSender） |

## 方案选择

**方案 A：定时短任务**。GitHub Actions cron 在交易时段每 20 分钟触发一次短任务，每次运行：拉取实时行情 → 计算指标 → 判断条件 → 推送告警 → 退出。通过 JSON 文件跟踪当日已告警股票。

优于方案 B（双 Job 分时段长跑，需要状态同步，6 小时限制尴尬）和方案 C（独立部署常驻服务，过度设计）。

## 架构总览

```
GitHub Actions cron (每20分钟，交易时段 UTC 1:00-7:59)
  ↓
python main.py --alert-monitor
  ↓
AlertMonitorService.run()                          [新增 src/services/alert_monitor.py]
  ├── 1. 加载去重状态 (data/alert_state.json)
  ├── 2. 遍历 STOCK_LIST，跳过今日已告警的
  ├── 3. 每只股票:
  │     ├── 拉取实时行情 (复用现有 data_provider)
  │     ├── 加载历史数据 (复用现有 history_loader)
  │     ├── 实时数据增强 (复用 pipeline._augment_historical_with_realtime)
  │     ├── 计算指标 (复用 StockTrendAnalyzer + 新增 MA120/MA250)
  │     └── 检查触发条件
  ├── 4. 对触发股票 → 格式化精简告警 → FeishuSender
  └── 5. 更新去重状态文件
```

核心原则：最大化复用现有模块，新增的只是一个「条件检查 + 去重 + 精简告警」的编排层。

## 改动文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `src/services/alert_monitor.py` | **新增** | 告警监控服务主逻辑 |
| `.github/workflows/alert_monitor.yml` | **新增** | 盘中定时触发工作流 |
| `.github/workflows/daily_analysis.yml` | **修改** | 停用每日 18:00 定时触发 |
| `src/stock_analyzer.py` | **修改** | 新增 MA120/MA250 计算及 TrendAnalysisResult 字段 |
| `main.py` | **修改** | 新增 `--alert-monitor` CLI 入口 |

## 模块设计

### 1. AlertMonitorService (`src/services/alert_monitor.py`)

核心编排类，负责串联整个告警流程。

```python
class AlertMonitorService:
    def __init__(self, config: Config):
        self.config = config
        self.stock_list = config.stock_list
        self.trend_analyzer = StockTrendAnalyzer()
        self.feishu_sender = FeishuSender(config)
        self.dedup_file = Path("data/alert_state.json")

    def run(self) -> list[str]:
        """Run alert monitoring, return triggered stock codes."""
        state = self._load_dedup_state()
        triggered_stocks = []

        for code in self.stock_list:
            if code in state["today"]:
                continue

            df = self._get_historical_data(code)
            quote = self._get_realtime_quote(code)
            if quote is None:
                continue
            df = self._augment_with_realtime(df, quote)

            trend = self.trend_analyzer.analyze(df, code)

            triggered, broken_mas = self._check_conditions(trend, df)
            if triggered:
                self._send_alert(code, quote, trend, broken_mas)
                state["today"].append(code)
                triggered_stocks.append(code)

        self._save_dedup_state(state)
        return triggered_stocks
```

**触发条件判断：**

```python
def _check_conditions(self, trend, df):
    # 1. MACD 当日死叉：昨天 DIF > DEA 且今天 DIF < DEA
    if len(df) < 2:
        return False, []
    dif = df['MACD_DIF']
    dea = df['MACD_DEA']
    macd_death_cross = (
        dif.iloc[-2] > dea.iloc[-2] and dif.iloc[-1] < dea.iloc[-1]
    )

    # 2. RSI_6 < 20
    rsi_critical = trend.rsi_6 < 20

    # 3. 跌破均线
    close = df['close'].iloc[-1]
    broken_mas = []
    for period in [20, 60, 120, 250]:
        ma_val = getattr(trend, f'ma{period}', None)
        if ma_val is not None and close < ma_val:
            broken_mas.append(period)

    triggered = macd_death_cross and rsi_critical and len(broken_mas) > 0
    return triggered, broken_mas
```

**去重状态文件** `data/alert_state.json`：

```json
{
  "2026-05-31": ["600519", "hk00700"],
  "2026-05-30": ["000001"]
}
```

自动清理 7 天前的旧记录，防止文件无限增长。

**告警消息格式**（精简版 Feishu 交互式卡片）：

```
⚠️ 盘中告警: 600519 贵州茅台
━━━━━━━━━━━━━━━━━━
MACD 死叉: DIF=12.35 DEA=14.20
RSI(6): 18.2 (严重超卖)
最新价: 1520.00 (-3.2%)
━━━━━━━━━━━━━━━━━━
跌破均线: MA20 MA60
    MA20=1550.00 (跌破 -1.9%)
    MA60=1580.00 (跌破 -3.8%)
```

### 2. StockTrendAnalyzer 改动 (`src/stock_analyzer.py`)

`_calculate_mas` 方法追加：

```python
if len(df) >= 120:
    df['MA120'] = df['close'].rolling(window=120).mean()
if len(df) >= 250:
    df['MA250'] = df['close'].rolling(window=250).mean()
```

`TrendAnalysisResult` dataclass 追加：
```python
ma120: Optional[float] = None
ma250: Optional[float] = None
```

`to_dict()` 方法追加对应序列化。

### 3. GitHub Actions 工作流 (`.github/workflows/alert_monitor.yml`)

```yaml
name: Alert Monitor
on:
  schedule:
    - cron: '*/20 1-7 * * 1-5'   # UTC 1:00-7:59 = Beijing 9:00-15:59, Mon-Fri
  workflow_dispatch:

jobs:
  alert-monitor:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    concurrency:
      group: alert-monitor
      cancel-in-progress: true
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'
      - run: pip install -r requirements.txt
      - run: python main.py --alert-monitor
        env:
          FEISHU_WEBHOOK_URL: ${{ secrets.FEISHU_WEBHOOK_URL }}
          FEISHU_WEBHOOK_SECRET: ${{ secrets.FEISHU_WEBHOOK_SECRET }}
          STOCK_LIST: ${{ vars.STOCK_LIST || secrets.STOCK_LIST }}
          # ... 其他数据源、DB 环境变量
```

### 4. CLI 入口 (`main.py`)

```python
if args.alert_monitor:
    from src.services.alert_monitor import AlertMonitorService
    config = Config.from_env()
    service = AlertMonitorService(config)
    triggered = service.run()
    logger.info(f"Alert monitor done, {len(triggered)} stocks triggered: {triggered}")
    return
```

`--alert-monitor` 模式下：
- 先检查交易日：非 A 股且非港股交易日则直接退出
- 如需更多粒度，可进一步检查当前北京时间是否在 9:30-16:00

## 数据流

```
STOCK_LIST (来自 .env / GitHub vars)
  → Config.stock_list
  → AlertMonitorService.run()
     ├── data_provider (实时行情)
     ├── history_loader (历史数据)
     ├── pipeline._augment_historical_with_realtime (实时增强)
     ├── StockTrendAnalyzer (MACD/RSI/MA)
     ├── 条件判断 (_check_conditions)
     ├── FeishuSender.send_to_feishu (飞书推送)
     └── data/alert_state.json (去重状态)
```

## 错误处理

- 实时行情拉取失败：静默跳过该股票（单只股票数据源失败不应中断全流程）
- 历史数据不足 250 条：MA120/MA250 为 None，判断时仅检查可用的均线
- Feishu 推送失败：记录日志，不影响其他股票；去重状态不更新（下次还尝试推送）
- 去重文件损坏：删除重建，当天可能重复推送优于丢失告警

## 验证方式

1. **本地模拟**：`python main.py --alert-monitor --dry-run` 打印触发情况而不推送
2. **手动触发**：GitHub Actions `workflow_dispatch` 手动运行验证
3. **单元测试**：`tests/test_alert_monitor.py` 覆盖触发条件判断、去重逻辑
4. **集成测试**：构造满足条件的 mock 数据，验证端到端推送

## 风险点

- GitHub Actions cron 不保证准时，可能延迟几分钟
- 去重依赖 `data/alert_state.json` 被 commit 回仓库，否则每次 checkout 丢失状态。替代方案：使用 GitHub Actions cache action 持久化
- 如果单次检查超过 10 分钟 timeout，会被强制终止
