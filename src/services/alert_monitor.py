# -*- coding: utf-8 -*-
"""盘中实时告警监控服务"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd

from src.config import Config
from src.stock_analyzer import StockTrendAnalyzer
from src.services.history_loader import load_history_df
from src.notification_sender.feishu_sender import FeishuSender
from src.core.trading_calendar import get_open_markets_today
from data_provider import DataFetcherManager

logger = logging.getLogger(__name__)

DEDUP_FILE = Path("data/alert_state.json")
HISTORY_DAYS = 300                # 250 天均线 + 余量
KEY_MA_PERIODS = [20, 60, 120, 250]


class AlertMonitorService:
    """盘中实时告警监控服务"""

    def __init__(self, config: Config, force_run: bool = False):
        self.config = config
        self.stock_list = config.stock_list
        self.force_run = force_run
        self.trend_analyzer = StockTrendAnalyzer()
        self.feishu_sender = FeishuSender(config)
        self.fetcher = DataFetcherManager()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> List[str]:
        """运行告警监控，返回触发告警的股票代码列表"""
        if not self.force_run:
            open_markets = get_open_markets_today()
            if not open_markets:
                logger.info("今天无开市市场，跳过告警监控（使用 --force-run 可强制执行）")
                return []
        else:
            open_markets = set()
            logger.info("强制运行模式：跳过交易日检查")

        logger.info(f"开市市场: {open_markets or '强制运行'}，开始告警监控，共 {len(self.stock_list)} 只股票")

        state = self._load_dedup_state()
        today_str = date.today().isoformat()
        already_alerted = set(state.get(today_str, []))
        triggered: List[str] = []

        for code in self.stock_list:
            if code in already_alerted:
                logger.debug(f"{code} 今日已告警，跳过")
                continue

            try:
                result = self._check_stock(code)
                if result is not None:
                    broken_mas, trend, quote = result
                    self._send_alert(code, quote, trend, broken_mas)
                    already_alerted.add(code)
                    triggered.append(code)
            except Exception:
                logger.exception(f"{code} 检查异常，跳过")

        state[today_str] = sorted(already_alerted)
        self._save_dedup_state(state)

        logger.info(f"告警监控完成，本次触发 {len(triggered)} 只: {triggered}")
        return triggered

    # ------------------------------------------------------------------
    # Per-stock pipeline
    # ------------------------------------------------------------------

    def _check_stock(self, code: str) -> Optional[Tuple[List[int], any, any]]:
        """检查单只股票是否触发告警。返回 (broken_mas, trend_result, quote) 或 None"""
        # 1. 实时行情
        quote = self.fetcher.get_realtime_quote(code)
        if quote is None or not quote.has_basic_data():
            logger.debug(f"{code} 无实时行情，跳过")
            return None

        # 2. 历史数据
        df, source = load_history_df(code, days=HISTORY_DAYS)
        if df is None or df.empty:
            logger.debug(f"{code} 无历史数据，跳过")
            return None

        # 3. 实时数据增强
        df = self._augment_with_realtime(df, quote)

        # 4. 技术指标
        trend = self.trend_analyzer.analyze(df, code)

        # 5. 条件判断
        broken_mas = self._check_conditions(trend, df)
        if broken_mas:
            return (broken_mas, trend, quote)
        return None

    # ------------------------------------------------------------------
    # Condition checks
    # ------------------------------------------------------------------

    def _check_conditions(self, trend, df: pd.DataFrame) -> List[int]:
        """
        检查告警条件:
        1. MACD 当日死叉 (前一日 DIF > DEA 且 当日 DIF < DEA)
        2. RSI_6 < 20
        3. 最新价 < MA20/60/120/250 中至少一条
        """
        if len(df) < 2:
            return []

        # 自行计算 MACD（analyzer 内部计算了但不返回 DataFrame）
        ema_fast = df['close'].ewm(span=12, adjust=False).mean()
        ema_slow = df['close'].ewm(span=26, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=9, adjust=False).mean()

        # 当日死叉
        death_cross = (
            float(dif.iloc[-2]) > float(dea.iloc[-2])
            and float(dif.iloc[-1]) < float(dea.iloc[-1])
        )
        if not death_cross:
            return []

        # RSI_6 < 20
        if trend.rsi_6 >= 20:
            return []

        # 跌破均线
        close = float(df['close'].iloc[-1])
        broken = []
        for period in KEY_MA_PERIODS:
            ma_val = getattr(trend, f'ma{period}', None)
            if ma_val is not None and ma_val > 0 and close < ma_val:
                broken.append(period)

        return broken

    # ------------------------------------------------------------------
    # Realtime augmentation
    # ------------------------------------------------------------------

    def _augment_with_realtime(self, df: pd.DataFrame, quote) -> pd.DataFrame:
        """用实时行情增强历史 DataFrame 的最后一条记录"""
        today = pd.Timestamp(date.today())

        df = df.copy()
        df = df.sort_values('date').reset_index(drop=True)

        last_date = df['date'].iloc[-1]
        if isinstance(last_date, pd.Timestamp):
            last_date = last_date.date()
        elif isinstance(last_date, datetime):
            last_date = last_date.date()
        elif isinstance(last_date, str):
            last_date = datetime.fromisoformat(last_date).date()

        if last_date == today.date():
            # 覆盖最后一行
            idx = len(df) - 1
            df.loc[idx, 'close'] = float(quote.price)
            if quote.open_price is not None:
                df.loc[idx, 'open'] = quote.open_price
            if quote.high is not None:
                df.loc[idx, 'high'] = quote.high
            if quote.low is not None:
                df.loc[idx, 'low'] = quote.low
            if quote.volume is not None:
                df.loc[idx, 'volume'] = quote.volume
            if quote.amount is not None:
                df.loc[idx, 'amount'] = quote.amount
            if quote.change_pct is not None:
                df.loc[idx, 'pct_chg'] = quote.change_pct
        else:
            # 追加新行
            new_row = {
                'date': today,
                'open': quote.open_price if quote.open_price is not None else quote.price,
                'high': quote.high if quote.high is not None else quote.price,
                'low': quote.low if quote.low is not None else quote.price,
                'close': float(quote.price),
                'volume': quote.volume or 0,
                'amount': quote.amount or 0.0,
                'pct_chg': quote.change_pct or 0.0,
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        return df

    # ------------------------------------------------------------------
    # Alert formatting & sending
    # ------------------------------------------------------------------

    def _send_alert(self, code: str, quote, trend, broken_mas: List[int]):
        """格式化并发送精简版飞书告警"""
        # 股票名优先级: 实时行情名 > 映射表名
        name = (quote.name or "").strip()
        if not name or name == code:
            name = self.fetcher.get_stock_name(code, allow_realtime=False) or ""

        price = quote.price or trend.current_price
        change_pct = quote.change_pct or 0.0

        # 均线明细
        ma_lines: List[str] = []
        for period in broken_mas:
            ma_val = getattr(trend, f'ma{period}', None)
            if ma_val and ma_val > 0:
                bias = (price - ma_val) / ma_val * 100
                ma_lines.append(f"    MA{period}={ma_val:.2f} (跌破 {bias:.1f}%)")

        content = (
            f"⚠️ **盘中告警: {code} {name}**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"MACD 死叉: DIF={trend.macd_dif:.2f} DEA={trend.macd_dea:.2f}\n"
            f"RSI(6): {trend.rsi_6:.1f} (严重超卖)\n"
            f"最新价: {price:.2f} ({change_pct:+.2f}%)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"跌破均线: {' '.join(f'MA{p}' for p in broken_mas)}\n"
            + "\n".join(ma_lines)
        )

        success = self.feishu_sender.send_to_feishu(content)
        if success:
            logger.info(f"{code} 告警已推送")
        else:
            logger.warning(f"{code} 飞书推送失败")

    # ------------------------------------------------------------------
    # Dedup state persistence
    # ------------------------------------------------------------------

    def _load_dedup_state(self) -> dict:
        """加载去重状态，自动清理 7 天前记录"""
        try:
            if DEDUP_FILE.exists():
                state = json.loads(DEDUP_FILE.read_text(encoding='utf-8'))
            else:
                return {}
        except (json.JSONDecodeError, OSError):
            logger.warning("去重文件损坏，重建")
            return {}

        cutoff = date.today()
        keys_to_keep = []
        for k in list(state.keys()):
            try:
                d = date.fromisoformat(k)
                if (cutoff - d).days <= 7:
                    keys_to_keep.append(k)
            except (ValueError, TypeError):
                pass

        return {k: state[k] for k in keys_to_keep}

    def _save_dedup_state(self, state: dict):
        """保存去重状态"""
        DEDUP_FILE.parent.mkdir(parents=True, exist_ok=True)
        DEDUP_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
