import sqlite3
import threading
from pathlib import Path
from loguru import logger
from datetime import datetime
import traceback

class PnLTracker:
    """
    一个线程安全的PnL记录器，使用SQLite来持久化数据。
    每个策略实例都应该有自己的PnLTracker实例，以策略名称来区分数据。
    """
    _lock = threading.Lock()

    def __init__(self, db_path: str, strategy_name: str):
        self.db_path = Path(db_path)
        self.strategy_name = strategy_name
        self._ensure_db_and_tables()

    def _get_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(self.db_path, timeout=10)

    def _ensure_db_and_tables(self):
        """确保数据库文件和表结构存在"""
        with self._lock:
            # 在连接之前，确保数据库文件所在的目录存在
            try:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.critical(f"无法创建数据库目录 {self.db_path.parent}: {e}")
                # 抛出异常或退出，因为没有数据库无法继续
                raise

            with self._get_connection() as conn:
                cursor = conn.cursor()
                # 创建 trades 表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    amount REAL NOT NULL,
                    fee_cost REAL NOT NULL,
                    fee_currency TEXT NOT NULL
                )
                ''')
                # 创建 funding_payments 表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS funding_payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL
                )
                ''')
                # 创建 funding_arbitrage_log 表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS funding_arbitrage_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_rate REAL NOT NULL,
                    funding_timestamp INTEGER NOT NULL,
                    position_size_usd REAL NOT NULL,
                    open_price REAL,
                    close_price REAL,
                    amount REAL,
                    trade_pnl REAL,
                    open_fee REAL,
                    close_fee REAL,
                    funding_payment REAL,
                    net_pnl REAL,
                    status TEXT NOT NULL,
                    notes TEXT
                )
                ''')
                conn.commit()

    def record_trade(self, trade_info: dict):
        """
        记录一笔成交。
        trade_info 应该包含: timestamp, symbol, side, price, amount, fee_cost, fee_currency
        """
        sql = '''
        INSERT INTO trades (strategy_name, timestamp, symbol, side, price, amount, fee_cost, fee_currency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        try:
            # 优先使用 'timestamp', 如果为 None, 则使用 'lastTradeTimestamp'
            timestamp_ms = trade_info.get('timestamp')
            if timestamp_ms is None:
                timestamp_ms = trade_info.get('lastTradeTimestamp')
                if timestamp_ms:
                    logger.debug(f"[{self.strategy_name}] 'timestamp'为空, 使用'lastTradeTimestamp' ({timestamp_ms}) 作为成交时间。")
                else:
                    logger.error(f"[{self.strategy_name}] 成交记录 {trade_info.get('id')} 中缺少 'timestamp' 和 'lastTradeTimestamp'，无法记录。")
                    return # 无法记录，直接返回

            fee_cost = 0.0
            fee_currency = None

            fee_info = trade_info.get('fee')
            if fee_info and isinstance(fee_info, dict):
                fee_cost = fee_info.get('cost')
                fee_currency = fee_info.get('currency')
            else:
                logger.warning(f"[{self.strategy_name}] 成交记录 {trade_info.get('id')} 中缺少手续费(fee)信息，将暂时记为0。")
            
            # 确保即使手续费信息部分存在，我们也不会出错
            if fee_cost is None:
                fee_cost = 0.0

            if fee_currency is None:
                # 当手续费信息不存在时，我们假设手续费为0，但仍然需要一个货币单位。
                # 我们从交易对 symbol 中推断计价货币, e.g., 'ORDI/USDC:USDC' -> 'USDC'
                try:
                    symbol = trade_info.get('symbol', '')
                    # 适用于 'BASE/QUOTE:SETTLE' 和 'BASE/QUOTE' 两种格式
                    fee_currency = symbol.split('/')[1].split(':')[0]
                    logger.debug(f"[{self.strategy_name}] 手续费货币未提供, 从 symbol '{symbol}' 推断为 '{fee_currency}'。")
                except IndexError:
                    logger.error(f"无法从 symbol '{trade_info.get('symbol')}' 中推断手续费货币, 将使用 'N/A'。")
                    fee_currency = 'N/A' # 使用一个占位符以避免数据库错误

            with self._lock:
                with self._get_connection() as conn:
                    conn.execute(sql, (
                        self.strategy_name,
                        timestamp_ms,
                        trade_info['symbol'],
                        trade_info['side'],
                        trade_info['price'],
                        trade_info['amount'],
                        fee_cost,
                        fee_currency
                    ))
                    conn.commit()
            logger.success(f"[{self.strategy_name}] 已成功记录成交: {trade_info['side']} {trade_info['amount']} {trade_info['symbol']} @ {trade_info['price']}")
        except Exception as e:
            logger.error(f"[{self.strategy_name}] 记录成交失败: {e}")
            logger.error(traceback.format_exc())
            logger.error(f"导致失败的成交信息: {trade_info}")

    def record_funding_payment(self, payment_info: dict):
        """
        记录一笔资金费用。
        payment_info 应该包含: timestamp, symbol, amount, currency
        """
        sql = '''
        INSERT INTO funding_payments (strategy_name, timestamp, symbol, amount, currency)
        VALUES (?, ?, ?, ?, ?)
        '''
        try:
            # 首先检查是否已存在相同的记录，避免重复
            with self._lock:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1 FROM funding_payments WHERE strategy_name = ? AND timestamp = ? AND symbol = ? AND amount = ?",
                                   (self.strategy_name, payment_info['timestamp'], payment_info['symbol'], payment_info['amount']))
                    if cursor.fetchone():
                        # logger.debug(f"[{self.strategy_name}] 跳过重复的资金费用记录。")
                        return

                    conn.execute(sql, (
                        self.strategy_name,
                        payment_info['timestamp'],
                        payment_info['symbol'],
                        payment_info['amount'],
                        payment_info['currency']
                    ))
                    conn.commit()
            logger.success(f"[{self.strategy_name}] 已成功记录资金费用: {payment_info['amount']} {payment_info['currency']} for {payment_info['symbol']}")
        except Exception as e:
            logger.error(f"[{self.strategy_name}] 记录资金费用失败: {e}")

    def record_arbitrage_event(self, event_data: dict):
        """
        记录一次完整的资金费率套利事件。
        """
        sql = '''
        INSERT INTO funding_arbitrage_log (
            strategy_name, timestamp, exchange, symbol, funding_rate, funding_timestamp, 
            position_size_usd, open_price, close_price, amount, trade_pnl, 
            open_fee, close_fee, funding_payment, net_pnl, status, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        try:
            with self._lock:
                with self._get_connection() as conn:
                    params = (
                        self.strategy_name,
                        event_data.get('timestamp'),
                        event_data.get('exchange'),
                        event_data.get('symbol'),
                        event_data.get('funding_rate'),
                        event_data.get('funding_timestamp'),
                        event_data.get('position_size_usd'),
                        event_data.get('open_price'),
                        event_data.get('close_price'),
                        event_data.get('amount'),
                        event_data.get('trade_pnl'),
                        event_data.get('open_fee'),
                        event_data.get('close_fee'),
                        event_data.get('funding_payment'),
                        event_data.get('net_pnl'),
                        event_data.get('status'),
                        event_data.get('notes')
                    )
                    conn.execute(sql, params)
                    conn.commit()
            logger.info(f"[{self.strategy_name}] 已成功记录套利事件 for {event_data.get('symbol')} on {event_data.get('exchange')}.")
        except Exception as e:
            logger.error(f"[{self.strategy_name}] 记录套利事件失败: {e}")
            logger.error(traceback.format_exc())
            logger.error(f"导致失败的事件数据: {event_data}")

    def calculate_pnl(self):
        """计算并返回详细的已实现盈亏"""
        pnl_details = {
            'grid_profit': 0.0,
            'total_fees': 0.0,
            'total_funding': 0.0,
            'net_profit': 0.0,
            'currency': 'USDC' # 假设默认计价单位
        }
        
        try:
            with self._lock:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # 1. 计算网格利润
                    cursor.execute("SELECT side, price, amount FROM trades WHERE strategy_name = ?", (self.strategy_name,))
                    trades = cursor.fetchall()
                    realized_profit = sum([-t[2] * t[1] if t[0] == 'buy' else t[2] * t[1] for t in trades])
                    pnl_details['grid_profit'] = realized_profit

                    # 2. 计算总手续费
                    cursor.execute("SELECT SUM(fee_cost) FROM trades WHERE strategy_name = ? AND fee_currency = ?", (self.strategy_name, pnl_details['currency']))
                    total_fees = cursor.fetchone()[0]
                    pnl_details['total_fees'] = total_fees if total_fees is not None else 0.0

                    # 3. 计算总资金费用
                    cursor.execute("SELECT SUM(amount) FROM funding_payments WHERE strategy_name = ? AND currency = ?", (self.strategy_name, pnl_details['currency']))
                    total_funding = cursor.fetchone()[0]
                    pnl_details['total_funding'] = total_funding if total_funding is not None else 0.0

            # 4. 计算净利润
            pnl_details['net_profit'] = pnl_details['grid_profit'] - pnl_details['total_fees'] + pnl_details['total_funding']
            return pnl_details
            
        except Exception as e:
            logger.error(f"[{self.strategy_name}] 计算PnL失败: {e}")
            return pnl_details # 返回默认值 