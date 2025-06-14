import asyncio
from .base_strategy import BaseStrategy
from ..pnl_tracker import PnLTracker

class GridStrategy(BaseStrategy):
    """
    永续合约网格交易策略。
    继承自BaseStrategy，实现了具体的策略逻辑。
    """
    def __init__(self, strategy_id: str, config: dict, pnl_tracker: PnLTracker, shared_state: dict):
        super().__init__(strategy_id, config, pnl_tracker, shared_state)
        self.symbol = self.config.get('symbol')
        if not self.symbol:
            raise ValueError(f"策略 {self.strategy_id} 的配置中缺少 'symbol'。")
            
        self.grid_range_low = self.config['grid_range_low']
        self.grid_range_high = self.config['grid_range_high']
        self.grid_levels = self.config['grid_levels']

    async def run(self):
        """策略主运行逻辑"""
        self.logger.info("启动网格策略...")
        
        # 1. 初始化和设置
        if not await self._setup_symbol():
            return
            
        await self._cancel_all_open_orders()

        # 2. 创建初始网格
        try:
            ticker = await self.exchange.fetch_ticker(self.symbol)
            last_price = ticker['last']
            if last_price is None:
                self.logger.error("无法获取最新价格。")
                return
            self.logger.info(f"获取到最新价格: {last_price}")
            await self._place_initial_grid(last_price)
        except Exception as e:
            self.logger.error(f"创建初始网格失败: {e}")
            return
            
        # 3. 启动健康检查和WebSocket监听的并发任务
        health_check_task = asyncio.create_task(self._health_check_loop())
        ws_listener_task = asyncio.create_task(self._ws_listener_loop())
        funding_fee_task = asyncio.create_task(self._funding_fee_loop())
        pnl_report_task = asyncio.create_task(self._pnl_report_loop())

        await asyncio.gather(health_check_task, ws_listener_task, funding_fee_task, pnl_report_task)

    async def _ws_listener_loop(self):
        """通过WebSocket监听订单成交"""
        self.logger.info("启动WebSocket订单监听器...")
        while True: # TODO: Add a proper running flag check
            try:
                orders = await self.exchange.watch_orders(self.symbol)
                for order in orders:
                    if order['status'] == 'closed' and order['filled'] > 0:
                        # 记录成交
                        self.pnl_tracker.record_trade(order)
                        await self._handle_filled_order(order)
            except asyncio.CancelledError:
                self.logger.info("WebSocket监听任务被取消。")
                break
            except Exception as e:
                self.logger.error(f"WebSocket监听时发生错误: {e}")
                await asyncio.sleep(15)

    async def _health_check_loop(self):
        """定期健康检查的主循环"""
        interval = self.config['params']['health_check_interval_seconds']
        self.logger.info(f"启动健康检查循环，间隔: {interval}秒。")
        while True: # TODO: Add a proper running flag check
            await asyncio.sleep(interval)
            self.logger.info("--- 开始定期健康检查 ---")
            try:
                uniformity = await self._check_grid_uniformity()
                threshold = self.config['params']['uniformity_threshold']
                if uniformity < threshold:
                    self.logger.warning(f"网格均匀性 ({uniformity:.2%}) 低于阈值 ({threshold:.2%})，将重置网格。")
                    await self._reset_grid()
            except Exception as e:
                self.logger.error(f"在健康检查循环中发生错误: {e}")
    
    async def _funding_fee_loop(self):
        """定期获取并记录资金费用"""
        self.logger.info("启动资金费用记录循环...")
        while True:
             await asyncio.sleep(3600) # 每小时检查一次
             try:
                # 获取最近2小时的资金费历史，避免遗漏
                since = self.exchange.milliseconds() - 2 * 60 * 60 * 1000 
                funding_history = await self.exchange.fetch_funding_history(self.symbol, since=since)
                for payment in funding_history:
                    self.pnl_tracker.record_funding_payment({
                        'timestamp': payment['timestamp'],
                        'symbol': payment['symbol'],
                        'amount': payment['amount'],
                        'currency': payment['info']['fundingAsset'] # 从info中获取更准确的资产信息
                    })
             except Exception as e:
                 self.logger.error(f"获取资金费用失败: {e}")

    async def _pnl_report_loop(self):
        """定期计算并报告PnL"""
        self.logger.info("启动PnL报告循环...")
        while True:
            await asyncio.sleep(300) # 每5分钟报告一次
            pnl = self.pnl_tracker.calculate_pnl()
            
            report = f"\n--- 收益报告: {self.config['name']} ---\n"
            report += f"- 已实现网格利润: {pnl['grid_profit']:.4f} {pnl['currency']}\n"
            report += f"- 累计交易手续费: {-pnl['total_fees']:.4f} {pnl['currency']}\n"
            report += f"- 累计资金费用:    {pnl['total_funding']:.4f} {pnl['currency']}\n"
            report += "------------------------------------\n"
            report += f"- 策略总已实现盈亏: {pnl['net_profit']:.4f} {pnl['currency']}\n"
            self.logger.info(report)

    async def _reset_grid(self):
        """重置网格"""
        await self._cancel_all_open_orders()
        trades = await self.exchange.fetch_trades(self.symbol, limit=1)
        if trades:
            center_price = trades[0]['price']
            self.logger.info(f"使用最新成交价 {center_price} 作为新中心价。")
            await self._place_initial_grid(center_price)
        else:
            self.logger.error("无法获取成交价来重置网格。")

    async def _setup_symbol(self):
        """设置杠杆和持仓模式"""
        leverage = self.config['params']['leverage']
        try:
            # Set margin mode
            try:
                await self.exchange.set_margin_mode('cross', self.symbol)
                self.logger.info("已成功设置为全仓模式。")
            except Exception as e:
                if 'No need to change margin type' in str(e):
                    self.logger.warning("无需更改，当前已是全仓模式。")
                else: raise
            # Set position mode
            try:
                await self.exchange.set_position_mode(hedged=False, symbol=self.symbol)
                self.logger.info("已成功设置为单向持仓模式。")
            except Exception as e:
                if '-4059' in str(e) or 'No need to change position side' in str(e):
                    self.logger.warning("无需更改，当前已是单向持仓模式。")
                else: raise
            # Set leverage
            await self.exchange.set_leverage(leverage, self.symbol)
            self.logger.info(f"杠杆已设置为 {leverage}x。")
            return True
        except Exception as e:
            self.logger.error(f"设置失败: {e}")
            return False

    async def _cancel_all_open_orders(self):
        """取消所有挂单"""
        try:
            open_orders = await self.exchange.fetch_open_orders(self.symbol)
            if open_orders:
                cancel_tasks = [self.exchange.cancel_order(order['id'], self.symbol) for order in open_orders]
                await asyncio.gather(*cancel_tasks)
                self.logger.info(f"已取消 {len(open_orders)} 个挂单。")
            else:
                self.logger.info("没有需要取消的挂单。")
        except Exception as e:
            self.logger.error(f"取消挂单失败: {e}")

    def _get_market_precision(self):
        market = self.exchange.markets[self.symbol]
        return market['precision']['price'], market['precision']['amount']

    async def _place_initial_grid(self, center_price):
        """创建初始网格订单"""
        params = self.config['params']
        spacing = params['spacing']
        value = params['order_value']
        levels = params['grid_levels']
        
        fixed_amount = value / center_price
        
        orders_to_create = []
        # Buy Orders
        for i in range(1, levels + 1):
            price = center_price - i * spacing
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'buy', 
                                     'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount), 
                                     'price': self.exchange.price_to_precision(self.symbol, price)})
        # Sell Orders
        for i in range(1, levels + 1):
            price = center_price + i * spacing
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'sell',
                                     'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount),
                                     'price': self.exchange.price_to_precision(self.symbol, price)})

        # Log and submit
        self._log_grid_plan(orders_to_create)
        self.logger.info(f"准备创建 {len(orders_to_create)} 个数量为 {self.exchange.amount_to_precision(self.symbol, fixed_amount)} 的初始网格订单。")
        await self._batch_submit_orders(orders_to_create)
    
    def _log_grid_plan(self, orders):
        log_message = f"即将创建的初始网格订单列表:\n"
        log_message += "--- 买单 (Buy Orders) ---\n"
        buy_orders = sorted([o for o in orders if o['side'] == 'buy'], key=lambda x: x['price'], reverse=True)
        for order in buy_orders: log_message += f"  - Side: {order['side']}, Price: {order['price']}, Amount: {order['amount']}\n"
        log_message += "--- 卖单 (Sell Orders) ---\n"
        sell_orders = sorted([o for o in orders if o['side'] == 'sell'], key=lambda x: x['price'])
        for order in sell_orders: log_message += f"  - Side: {order['side']}, Price: {order['price']}, Amount: {order['amount']}\n"
        self.logger.info(log_message.strip())

    async def _batch_submit_orders(self, orders):
        """分批提交订单"""
        for i in range(0, len(orders), 5):
            batch = orders[i:i+5]
            try:
                created_orders = await self.exchange.create_orders(batch)
                self.logger.success(f"成功提交一批 {len(created_orders)} 个订单。")
                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error(f"批量提交订单失败: {e}")

    async def _handle_filled_order(self, filled_order):
        """处理成交订单"""
        params = self.config['params']
        spacing = params['spacing']
        levels = params['grid_levels']
        value = params['order_value']
        
        fill_price = filled_order['price']
        fill_side = filled_order['side']
        self.logger.info(f"检测到订单成交: {fill_side.upper()} @ {fill_price}")

        fixed_amount = value / fill_price
        orders_to_create = []

        if fill_side == 'buy':
            # New sell order
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'sell', 'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount), 'price': self.exchange.price_to_precision(self.symbol, fill_price + spacing)})
            # New buy order
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'buy', 'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount), 'price': self.exchange.price_to_precision(self.symbol, fill_price - levels * spacing)})
            await self._batch_submit_orders(orders_to_create)
            
            # Cancel highest sell
            open_orders = await self.exchange.fetch_open_orders(self.symbol)
            sell_orders = [o for o in open_orders if o['side'] == 'sell']
            if sell_orders:
                highest_sell = max(sell_orders, key=lambda o: o['price'])
                await self.exchange.cancel_order(highest_sell['id'], self.symbol)
                self.logger.info(f"取消价格最高的空单: {highest_sell['price']}")
        elif fill_side == 'sell':
            # New buy order
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'buy', 'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount), 'price': self.exchange.price_to_precision(self.symbol, fill_price - spacing)})
            # New sell order
            orders_to_create.append({'symbol': self.symbol, 'type': 'limit', 'side': 'sell', 'amount': self.exchange.amount_to_precision(self.symbol, fixed_amount), 'price': self.exchange.price_to_precision(self.symbol, fill_price + levels * spacing)})
            await self._batch_submit_orders(orders_to_create)

            # Cancel lowest buy
            open_orders = await self.exchange.fetch_open_orders(self.symbol)
            buy_orders = [o for o in open_orders if o['side'] == 'buy']
            if buy_orders:
                lowest_buy = min(buy_orders, key=lambda o: o['price'])
                await self.exchange.cancel_order(lowest_buy['id'], self.symbol)
                self.logger.info(f"取消价格最低的多单: {lowest_buy['price']}")

    async def _check_grid_uniformity(self):
        """检查网格均匀性"""
        levels = self.config['params']['grid_levels']
        try:
            open_orders = await self.exchange.fetch_open_orders(self.symbol)
            buy_orders = [o for o in open_orders if o['side'] == 'buy']
            sell_orders = [o for o in open_orders if o['side'] == 'sell']

            if not buy_orders or not sell_orders:
                self.logger.warning("网格订单不完整，买单/卖单为空。")
                return 0.0

            uniformity = min(len(buy_orders), len(sell_orders)) / levels
            self.logger.info(f"网格健康检查。买单: {len(buy_orders)}, 卖单: {len(sell_orders)}, 均匀性: {uniformity:.2%}")
            return uniformity
        except Exception as e:
            self.logger.error(f"网格检查失败: {e}")
            return 1.0 