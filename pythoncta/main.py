import ccxt.pro as ccxt
import asyncio
import json
import importlib
from loguru import logger
from pnl_tracker import PnLTracker

def load_config():
    """加载并合并配置文件"""
    try:
        with open('configs/global_config.json', 'r') as f:
            global_config = json.load(f)
        with open('configs/strategies.json', 'r') as f:
            strategies_config = json.load(f)
        
        # 将交易所信息合并到主配置中，方便后续使用
        global_config.update(strategies_config)
        return global_config
    except FileNotFoundError as e:
        logger.error(f"配置文件缺失: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"配置文件格式错误: {e}")
        return None

class TradingSystem:
    def __init__(self, config):
        self.config = config
        self.exchanges = {}
        self.running_strategies = []
        self.tasks = []

    async def initialize_exchanges(self):
        """根据配置初始化所有启用的交易所实例"""
        for ex_name, ex_config in self.config['exchanges'].items():
            if not ex_config.get('enabled', False):
                logger.warning(f"交易所 '{ex_name}' 已被禁用，跳过初始化。")
                continue
            
            try:
                exchange_class = getattr(ccxt, ex_config['ccxt_id'])
                exchange = exchange_class({
                    'apiKey': ex_config['apiKey'],
                    'secret': ex_config['secret'],
                    'options': {'defaultType': 'future'},
                })
                await exchange.load_markets()
                self.exchanges[ex_name] = exchange
                logger.success(f"交易所 '{ex_name}' 初始化成功。")
            except Exception as e:
                logger.error(f"交易所 '{ex_name}' 初始化失败: {e}")

    def load_strategies(self):
        """根据配置加载并实例化所有启用的策略"""
        for strategy_config in self.config['strategies']:
            if not strategy_config.get('enabled', False):
                logger.warning(f"策略 '{strategy_config['name']}' 已被禁用，跳过加载。")
                continue

            try:
                exchange_name = strategy_config['exchange']
                exchange_instance = self.exchanges.get(exchange_name)
                
                if not exchange_instance:
                    logger.error(f"策略 '{strategy_config['name']}' 所需的交易所 '{exchange_name}' 未启用或初始化失败。")
                    continue

                # 为该策略创建一个PnL追踪器实例
                pnl_tracker = PnLTracker(db_path="pnl.db", strategy_name=strategy_config['name'])

                # 动态导入策略类
                module_path, class_name = strategy_config['class_path'].rsplit('.', 1)
                module = importlib.import_module(module_path)
                StrategyClass = getattr(module, class_name)
                
                # 实例化策略，并传入pnl_tracker
                strategy_instance = StrategyClass(
                    exchange=exchange_instance, 
                    strategy_config=strategy_config, 
                    pnl_tracker=pnl_tracker
                )
                self.running_strategies.append(strategy_instance)
                
                strategy_log_level = strategy_config.get('log_level') or self.config['system']['log_level']
                logger.info(f"策略 '{strategy_config['name']}' 加载成功，日志级别: {strategy_log_level}。")

            except Exception as e:
                import traceback
                logger.error(f"策略 '{strategy_config['name']}' 加载失败: {e}")
                logger.error(traceback.format_exc())

    async def run(self):
        """启动所有策略的主循环"""
        if not self.running_strategies:
            logger.warning("没有已启用的策略可运行。程序将退出。")
            return
            
        self.tasks = [asyncio.create_task(strategy.run()) for strategy in self.running_strategies]
        logger.info(f"系统启动成功，共运行 {len(self.tasks)} 个策略。按 CTRL+C 停止。")
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def shutdown(self):
        """安全关闭所有交易所连接和任务"""
        logger.info("开始关闭系统...")
        
        # 取消所有策略任务
        for task in self.tasks:
            task.cancel()
        
        # 关闭所有交易所连接
        for ex_name, exchange in self.exchanges.items():
            try:
                await exchange.close()
                logger.info(f"交易所 '{ex_name}' 连接已关闭。")
            except Exception as e:
                logger.error(f"关闭交易所 '{ex_name}' 时出错: {e}")
        
        # 等待任务完成取消
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("系统已安全关闭。")

async def main():
    # 加载配置
    config = load_config()
    if not config:
        return

    # 配置全局日志
    logger.add(
        f"logs/system.log", 
        rotation="10 MB", 
        retention="7 days", 
        level=config['system']['log_level'],
        # 使用filter确保只有未被特定策略捕获的日志写入系统日志
        filter=lambda record: 'strategy' not in record['extra']
    )
    # 为每个策略配置专属的日志文件
    for strategy_config in config.get('strategies', []):
        if strategy_config.get('enabled', False):
            log_level = strategy_config.get('log_level') or config['system']['log_level']
            log_name = strategy_config.get('name', 'unnamed_strategy')
            logger.add(
                f"logs/{log_name}.log",
                rotation="10 MB",
                retention="7 days",
                level=log_level,
                filter=lambda record, name=log_name: record['extra'].get('strategy') == name
            )

    if not config['system'].get('enabled', False):
        logger.warning("系统在配置文件中被禁用。程序将退出。")
        return

    system = TradingSystem(config)
    
    try:
        await system.initialize_exchanges()
        system.load_strategies()
        await system.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("接收到停止信号。")
    finally:
        await system.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序已强制退出。") 