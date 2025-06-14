from abc import ABC, abstractmethod
from loguru import logger
from pnl_tracker import PnLTracker
import ccxt.async_support as ccxt

class BaseStrategy(ABC):
    """
    所有策略类的抽象基类。
    它定义了所有策略必须实现的通用接口。
    """
    def __init__(self, strategy_id: str, config: dict, pnl_tracker: PnLTracker, shared_state: dict):
        """
        初始化策略。

        :param strategy_id: 策略的唯一标识符。
        :param config: 该策略在strategies.json中的特定配置。
        :param pnl_tracker: 该策略专属的PnL记录器实例。
        :param shared_state: 包含全局配置（如API密钥）的共享状态字典。
        """
        self.strategy_id = strategy_id
        self.config = config
        self.pnl_tracker = pnl_tracker
        self.shared_state = shared_state
        self.logger = logger.bind(strategy_id=self.strategy_id)

    def get_exchange(self, exchange_id: str) -> ccxt.Exchange | None:
        """
        根据全局配置动态创建一个交易所实例。
        """
        exchange_config = self.shared_state.get('global_config', {}).get('exchanges', {}).get(exchange_id)
        if not exchange_config:
            self.logger.error(f"在 global_config.json 中未找到交易所 '{exchange_id}' 的配置。")
            return None
            
        try:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'apiKey': exchange_config.get('apiKey'),
                'secret': exchange_config.get('secret'),
                'password': exchange_config.get('password'),
                'options': {
                    'defaultType': 'swap',
                },
            })
            return exchange
        except (AttributeError, Exception) as e:
            self.logger.error(f"创建交易所 '{exchange_id}' 实例失败: {e}")
            return None

    @abstractmethod
    async def run(self):
        """
        策略的主运行逻辑。
        每个子策略都必须实现这个方法。
        """
        pass

    def _get_default_logger(self):
        """
        如果没提供专属logger，则返回一个绑定了策略名称的默认logger。
        """
        return logger.bind(strategy=self.config.get('name', 'UnnamedStrategy')) 