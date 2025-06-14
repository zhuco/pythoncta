import asyncio
import json
from pathlib import Path
from loguru import logger
import sys

from strategies.funding_rate_strategy import FundingRateStrategy
from pnl_tracker import PnLTracker

# 配置日志记录
log_path = Path("logs") / "funding_rate_strategy.log"
logger.add(log_path, rotation="10 MB", retention="7 days", level="DEBUG",
           format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}")

def load_config(path):
    """加载JSON配置文件"""
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"加载配置文件 {path} 失败: {e}")
        sys.exit(1)

async def main():
    """
    专门用于启动资金费率套利策略的主函数。
    """
    logger.info("启动资金费率套利策略专用运行程序...")
    
    global_config = load_config('configs/global_config.json')
    strategies_config_raw = load_config('configs/strategies.json')

    # 兼容 configs/strategies.json 顶层是字典或列表两种情况
    if isinstance(strategies_config_raw, dict) and 'strategies' in strategies_config_raw:
        strategies_config = strategies_config_raw['strategies']
    elif isinstance(strategies_config_raw, list):
        strategies_config = strategies_config_raw
    else:
        logger.error("configs/strategies.json 的格式不正确，应为一个列表或包含'strategies'键的字典。")
        return

    funding_strategy_config = None
    for config in strategies_config:
        if isinstance(config, dict) and config.get("strategy_class") == "FundingRateStrategy":
            funding_strategy_config = config
            break
            
    if not funding_strategy_config:
        logger.error("在 configs/strategies.json 中未找到 FundingRateStrategy 的配置。")
        return

    strategy_id = funding_strategy_config.get('strategy_id', 'FundingRateStrategy_default')
    
    # 初始化 PnL 追踪器
    pnl_tracker = PnLTracker(db_path="pnl.db", strategy_name=strategy_id)
    
    # 共享状态（如果需要）
    shared_state = {
        'global_config': global_config
    }
    
    logger.info(f"正在初始化策略: {strategy_id}")
    
    strategy = FundingRateStrategy(
        strategy_id=strategy_id,
        config=funding_strategy_config,
        pnl_tracker=pnl_tracker,
        shared_state=shared_state
    )
    
    try:
        await strategy.run()
    except asyncio.CancelledError:
        logger.info(f"策略 {strategy_id} 的主任务被取消。")
    except Exception as e:
        logger.error(f"策略 {strategy_id} 运行时发生未捕获的异常: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        logger.info(f"策略 {strategy_id} 已停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户通过 Ctrl+C 手动中断。") 