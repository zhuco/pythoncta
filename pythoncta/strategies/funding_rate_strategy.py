from .base_strategy import BaseStrategy
from loguru import logger
import asyncio
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import ccxt.async_support as ccxt
import time
import traceback

class FundingRateStrategy(BaseStrategy):
    """
    资金费率套利策略。

    核心逻辑：
    1. 定时（例如每小时的第58分钟）触发检查。
    2. 从所有启用的交易所并发获取所有永续合约的资金费率。
    3. 筛选出绝对值最高且满足阈值条件的交易对作为目标。
    4. 计算下单数量，确保满足交易所的精度和最小下单量要求。
    5. 根据资金费率的正负，计划在结算时间前特定秒数开立多单或空单。
    6. 计划在结算时间后特定毫秒数平掉仓位。
    7. 记录每次套利操作的详细结果到数据库。
    """
    def __init__(self, strategy_id: str, config: dict, pnl_tracker, shared_state: dict):
        super().__init__(strategy_id, config, pnl_tracker, shared_state)
        # 策略特定参数的初始化
        self.enabled_exchanges = self.config.get('enabled_exchanges', [])
        self.check_minute = self.config.get('check_minute', 58)
        self.rate_threshold = self.config.get('rate_threshold', 0.0025) # 0.25%
        self.position_size_usd = self.config.get('position_size_usd', 50)
        self.open_time_offset = self.config.get('open_time_offset_sec', 2) # 结算前2秒开仓
        self.close_time_offset = self.config.get('close_time_offset_ms', 10) # 结算后10毫秒平仓, 1ms太短可能失败
        self.quote_currency = self.config.get('quote_currency', 'USDT').upper()
        self.logger.info(f"[{self.strategy_id}] 资金费率套利策略初始化完成。")
        self.logger.info(f"[{self.strategy_id}] - 检查分钟: {self.check_minute}")
        self.logger.info(f"[{self.strategy_id}] - 费率阈值: {self.rate_threshold}")
        self.logger.info(f"[{self.strategy_id}] - 报价货币: {self.quote_currency}")
        self.logger.info(f"[{self.strategy_id}] - 开仓规模: ${self.position_size_usd}")
        self.logger.info(f"[{self.strategy_id}] - 开仓时间偏移: {self.open_time_offset}s")
        self.logger.info(f"[{self.strategy_id}] - 平仓时间偏移: {self.close_time_offset}ms")
        self.scheduler = AsyncIOScheduler()
        self.active_arbitrage_tasks = []


    async def run(self):
        """
        策略主循环。
        """
        self.logger.info(f"[{self.strategy_id}] 开始运行资金费率套利策略。")
        
        trigger = CronTrigger(minute=self.check_minute, second=0)
        self.scheduler.add_job(
            self._find_and_execute_arbitrage,
            trigger=trigger,
            name=f'{self.strategy_id}_check'
        )
        self.scheduler.start()
        self.logger.info(f"[{self.strategy_id}] 调度器已启动，将在每小时第 {self.check_minute} 分钟触发检查。")

        # 首次启动时立即执行一次检查
        await self._find_and_execute_arbitrage()

        try:
            while True:
                await asyncio.sleep(3600) # 保持主协程运行
        except asyncio.CancelledError:
            self.logger.info(f"[{self.strategy_id}] 策略任务被取消。")
        finally:
            if self.scheduler.running:
                self.scheduler.shutdown()
            self.logger.info(f"[{self.strategy_id}] 调度器已关闭。")

    async def _find_and_execute_arbitrage(self):
        """为每个交易所查找并执行最佳套利机会"""
        self.logger.info(f"[{self.strategy_id}] 开始执行资金费率检查...")

        # 清理已完成的任务
        self.active_arbitrage_tasks = [task for task in self.active_arbitrage_tasks if not task.done()]

        if self.active_arbitrage_tasks:
            self.logger.warning(f"[{self.strategy_id}] {len(self.active_arbitrage_tasks)} 个套利任务仍在进行中，跳过本次检查。")
            return

        opportunities = await self._get_best_opportunities_per_exchange()

        if not opportunities:
            self.logger.info(f"[{self.strategy_id}] 未找到任何交易所的套利机会。")
            return

        self.logger.info(f"[{self.strategy_id}] 共找到 {len(opportunities)} 个套利机会，将并发执行。")
        
        for opportunity in opportunities:
            self.logger.success(
                f"[{self.strategy_id}] 找到套利机会: "
                f"交易所={opportunity['exchange_id']}, "
                f"交易对={opportunity['symbol']}, "
                f"资金费率={opportunity['rate']:.6f}, "
                f"下次结算时间={datetime.fromtimestamp(opportunity['next_funding_timestamp'] / 1000, tz=timezone.utc)}"
            )
            task = asyncio.create_task(self._execute_arbitrage(opportunity))
            self.active_arbitrage_tasks.append(task)

    async def _get_best_opportunities_per_exchange(self):
        """并发获取所有交易所的资金费率，并为每个交易所返回最佳机会"""
        tasks = [self._fetch_rates_from_exchange(exchange_id) for exchange_id in self.enabled_exchanges]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        best_opportunities = []

        # 每个 'res' 对应一个交易所返回的所有合格机会列表
        for res in results:
            if isinstance(res, Exception):
                self.logger.error(f"[{self.strategy_id}] 获取费率时发生错误: {res}")
                continue
            if not res:
                continue

            best_for_exchange = None
            # 我们假设 res[0]['exchange_id'] 对于该列表中的所有项目都是相同的
            exchange_id = res[0]['exchange_id'] 
            max_abs_rate = 0
            
            for opp in res:
                abs_rate = abs(opp['rate'])
                if abs_rate > self.rate_threshold and abs_rate > max_abs_rate:
                    max_abs_rate = abs_rate
                    best_for_exchange = opp
            
            if best_for_exchange:
                best_opportunities.append(best_for_exchange)
        
        self.logger.info(f"[{self.strategy_id}] 筛选出 {len(best_opportunities)} 个交易所的最佳套利机会。")
        return best_opportunities

    async def _fetch_rates_from_exchange(self, exchange_id: str):
        """从单个交易所获取资金费率"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            self.logger.error(f"[{self.strategy_id}] 无法初始化交易所 {exchange_id}")
            return []
        
        try:
            await exchange.load_markets(True) # Force reload markets
            self.logger.info(f"[{self.strategy_id}] ==> [DIAGNOSTIC] 从 {exchange_id} 加载了 {len(exchange.markets)} 个市场。")

            funding_rates = await exchange.fetch_funding_rates()
            
            # 诊断日志: 打印获取到的费率记录数量和示例
            if funding_rates:
                self.logger.info(f"[{self.strategy_id}] ==> [DIAGNOSTIC] 从 {exchange_id} 获取到 {len(funding_rates)} 条原始费率记录. 示例 symbols: {list(funding_rates.keys())[:5]}")
                if exchange_id == 'binance':
                    try:
                        first_symbol = next(iter(funding_rates))
                        self.logger.info(f"[{self.strategy_id}] [DIAGNOSTIC] Binance first funding rate info for {first_symbol}: {funding_rates[first_symbol]}")
                    except StopIteration:
                        self.logger.warning(f"[{self.strategy_id}] [DIAGNOSTIC] Binance funding rates dict is empty, cannot log first element.")
            else:
                self.logger.warning(f"[{self.strategy_id}] ==> [DIAGNOSTIC] 从 {exchange_id} 未获取到任何原始费率记录。")

            opportunities = []
            has_logged_market_structure = False # 为诊断添加一个标志
            for symbol, rate_info in funding_rates.items():
                try:
                    market = exchange.market(symbol)

                    # 诊断日志: 打印第一个市场的数据结构
                    if not has_logged_market_structure and market:
                        self.logger.debug(f"[{self.strategy_id}] [DIAGNOSTIC] {exchange_id} 的第一个市场结构 ({symbol}): {market}")
                        has_logged_market_structure = True

                    # 检查是否是永续合约 (swap: true, expiry: None)
                    if not market or not market.get('swap') or market.get('expiry') is not None:
                        self.logger.debug(f"跳过 {symbol}: 不是永续合约 (swap: {market.get('swap')}, expiry: {market.get('expiry')}).")
                        continue
                    
                    # 检查结算货币是否为USDT
                    settle_currency = market.get('settle', '').upper()
                    if settle_currency != self.quote_currency:
                        self.logger.debug(f"跳过 {symbol}: 结算货币为 {settle_currency} 而非 {self.quote_currency}。")
                        continue

                    rate = float(rate_info.get('fundingRate'))
                    # 检查费率和下一次结算时间是否存在
                    # 兼容性修改：某些交易所(如Binance)可能将下次结算时间放在 fundingTimestamp
                    next_funding_ts = rate_info.get('nextFundingTimestamp') or rate_info.get('fundingTimestamp')
                    
                    if rate is not None and next_funding_ts is not None:
                        self.logger.debug(f"[{self.strategy_id}] ✔️ 添加有效候选交易对: {symbol} (费率: {rate})")
                        opportunities.append({
                            'exchange_id': exchange_id,
                            'symbol': symbol,
                            'rate': rate,
                            'mark_price': rate_info.get('markPrice'),
                            'next_funding_timestamp': next_funding_ts,
                        })
                except (ValueError, TypeError):
                    self.logger.warning(f"无法解析 {symbol} 的费率信息: {rate_info}")
                    continue
                except Exception as e:
                    self.logger.warning(f"[{self.strategy_id}] 处理 {symbol} 时出现问题，跳过. Error: {e}")
                    continue

            self.logger.info(f"[{self.strategy_id}] <== 从 {exchange_id} 成功筛选出 {len(opportunities)} 个 USDT 永续合约的费率信息。")
            return opportunities
        except Exception as e:
            self.logger.error(f"[{self.strategy_id}] <== 从 {exchange_id} 获取资金费率失败: {e}")
            return []
        finally:
            await exchange.close()

    async def _execute_arbitrage(self, opportunity: dict):
        """执行开仓、等待、平仓的完整套利流程"""
        exchange_id = opportunity['exchange_id']
        symbol = opportunity['symbol']
        exchange = self.get_exchange(exchange_id)
        event_data = {
            'timestamp': int(time.time() * 1000),
            'exchange': exchange_id,
            'symbol': symbol,
            'funding_rate': opportunity['rate'],
            'funding_timestamp': opportunity['next_funding_timestamp'],
            'position_size_usd': self.position_size_usd,
            'status': 'INITIATED',
            'notes': ''
        }

        try:
            if not exchange:
                raise Exception(f"无法初始化交易所 {exchange_id}")

            self.logger.info(f"[{self.strategy_id}] 正在为 {exchange_id} 加载市场...")
            await exchange.load_markets()
            self.logger.info(f"[{self.strategy_id}] {exchange_id} 市场加载完毕.")
            
            # 1. 时间同步
            exchange_time = await exchange.fetch_time()
            local_time = int(time.time() * 1000)
            time_diff = exchange_time - local_time
            self.logger.info(f"本地与交易所 {exchange_id} 时间差: {time_diff}ms")

            # 2. 计算精确的开平仓时间
            funding_ts = opportunity['next_funding_timestamp']
            open_ts = funding_ts - self.open_time_offset * 1000
            close_ts = funding_ts + self.close_time_offset
            
            # 3. 计算下单数量
            market = exchange.market(symbol)
            mark_price = opportunity['mark_price']
            if not mark_price:
                self.logger.info(f"[{self.strategy_id}] 机会中未包含标记价格，正在为 {symbol} 从交易所重新获取...")
                ticker = await exchange.fetch_ticker(symbol)
                
                # 优先使用标记价格，如果不存在则回退到最新成交价
                if ticker and ticker.get('markPrice') is not None:
                    mark_price = ticker['markPrice']
                    self.logger.info(f"[{self.strategy_id}] 成功获取标记价格: {mark_price}")
                elif ticker and ticker.get('last') is not None:
                    mark_price = ticker['last']
                    self.logger.warning(f"[{self.strategy_id}] 获取标记价格失败，回退使用最新成交价: {mark_price}")
                else:
                    error_msg = f"无法为 {symbol} 获取有效的标记价格或最新成交价。"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)

            amount_unadjusted = self.position_size_usd / mark_price
            amount = exchange.amount_to_precision(symbol, amount_unadjusted)
            
            if float(amount) <= 0:
                raise Exception(f"计算出的下单数量为0或负数 ({amount})")
            
            self.logger.info(f"[{self.strategy_id}] 计划交易: {symbol}, 数量={amount}, 标记价格={mark_price}")

            # 4. 调度开仓
            side = 'sell' if opportunity['rate'] > 0 else 'buy'
            
            # 为不同交易所构建特定参数
            open_params = {}
            if exchange_id == 'binance':
                open_params['positionSide'] = 'BOTH' # 单向持仓
            elif exchange_id == 'okx':
                # OKX在单向持仓模式下不需要特殊参数，但双向持仓需要 posSide
                # 这里假设为单向持仓模式 (net mode)
                pass

            await self._sleep_until(open_ts, time_diff)
            
            self.logger.info(f"[{self.strategy_id}] 时间到达，执行 {side} 开仓，{symbol}")
            open_order = await exchange.create_market_order(symbol, side, amount, params=open_params)
            self.logger.success(f"[{self.strategy_id}] 开仓订单已提交: {open_order['id']}")

            # 5. 调度平仓
            close_side = 'buy' if side == 'sell' else 'sell'
            
            close_params = {}
            if exchange_id == 'binance':
                close_params['positionSide'] = 'BOTH'
            elif exchange_id == 'okx':
                pass

            await self._sleep_until(close_ts, time_diff)
            
            self.logger.info(f"[{self.strategy_id}] 时间到达，执行 {close_side} 平仓，{symbol}")
            close_order = await exchange.create_market_order(symbol, close_side, amount, params=close_params)
            self.logger.success(f"[{self.strategy_id}] 平仓订单已提交: {close_order['id']}")
            
            # 6. PnL 分析与记录
            # (简化处理，实际可能需要等待成交回报或轮询)
            # 理想情况下需要获取开平仓的成交均价和手续费
            # 这里暂时使用标记价格做估算
            open_price = open_order.get('average', mark_price)
            close_price = close_order.get('average', mark_price)

            trade_pnl = (close_price - open_price) * float(amount) if side == 'buy' else (open_price - close_price) * float(amount)
            # funding_payment 的计算需要从交易所获取，此处为估算
            funding_payment = -1 * opportunity['rate'] * self.position_size_usd
            # 真实手续费需要从订单回报获取
            open_fee = open_order.get('fee', {}).get('cost', 0.0)
            close_fee = close_order.get('fee', {}).get('cost', 0.0)

            event_data.update({
                'open_price': open_price,
                'close_price': close_price,
                'amount': float(amount),
                'trade_pnl': trade_pnl,
                'open_fee': open_fee,
                'close_fee': close_fee,
                'funding_payment': funding_payment,
                'net_pnl': trade_pnl - open_fee - close_fee + funding_payment,
                'status': 'SUCCESS',
                'notes': 'PnL is an estimation.'
            })
            self.logger.success(f"[{self.strategy_id}] 套利完成: 净收益(估算) = {event_data['net_pnl']:.4f} USD")

        except Exception as e:
            self.logger.error(f"[{self.strategy_id}] 执行套利时发生严重错误: {e}")
            self.logger.error(traceback.format_exc())
            event_data.update({ 'status': 'FAILED', 'notes': str(e) })
        finally:
            self.pnl_tracker.record_arbitrage_event(event_data)
            if exchange:
                await exchange.close()

    async def _sleep_until(self, timestamp_ms, time_diff):
        """根据与服务器的时间差，精确休眠到目标时间戳"""
        now_ms = int(time.time() * 1000)
        target_ms_local = timestamp_ms - time_diff
        sleep_duration_ms = target_ms_local - now_ms
        if sleep_duration_ms > 0:
            self.logger.debug(f"计划在 {sleep_duration_ms / 1000:.2f} 秒后执行下一步操作...")
            await asyncio.sleep(sleep_duration_ms / 1000) 