# 币安永续合约网格交易机器人

这是一个使用 Python 和 CCXT 库实现的，针对币安永续合约（Binance Futures）的多币种网格交易机器人。

## 特性

- **多币种支持**: 可在 `config.json` 中配置多个交易对同时运行。
- **异步处理**: 基于 `asyncio` 实现，高效处理网络请求和 WebSocket 数据。
- **动态网格**: 通过 WebSocket 实时订阅订单成交，自动补单并维持网格结构。
- **批量操作**: 批量提交和取消订单，减少延迟。
- **自动健康检查**: 定期检查网格的完整性和均匀性，当低于阈值时自动重置网格。
- **安全关闭**: 捕获退出信号 (Ctrl+C)，自动取消所有挂单后安全退出。
- **可配置**: 所有策略参数（杠杆、网格间距、订单价值等）均在 `config.json` 中配置。

## 设置步骤

### 1. 创建并激活虚拟环境

为了保持项目依赖的隔离，建议使用虚拟环境。

**Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python -m venv venv
.\venv\Scripts\activate
```

### 2. 安装依赖

激活虚拟环境后，使用 pip 安装所有必需的库：

```bash
pip install -r requirements.txt
```

### 3. 配置策略

打开 `config.json` 文件并进行如下配置：

- **填写 API 密钥**:
  将 `YOUR_API_KEY` 和 `YOUR_SECRET_KEY` 替换为您自己的币安 API 密钥。
  ```json
  "binance": {
    "apiKey": "YOUR_API_KEY",
    "secret": "YOUR_SECRET_KEY"
  }
  ```
  **注意**: 为了安全，请确保您的 API 密钥已启用"允许合约交易"，但**不要**启用"允许提现"。

- **配置交易对**:
  您可以在 `symbols` 列表中添加或修改您想运行的交易对及其参数。
  ```json
  "symbols": [
    {
      "symbol": "ENA/USDC",
      "spacing": 0.003,
      "order_value": 10,
      "grid_levels": 6,
      "leverage": 15
    },
    {
      "symbol": "BTC/USDT",
      "spacing": 100,
      "order_value": 20,
      "grid_levels": 10,
      "leverage": 20
    }
  ]
  ```
  - `symbol`: 交易对，必须是 `ccxt` 支持的格式（例如 `BTC/USDT`）。
  - `spacing`: 网格间距（价格）。
  - `order_value`: 每个网格订单的价值（以计价货币计算，如 USDT 或 USDC）。
  - `grid_levels`: 单边网格数量（例如 `6` 表示上方 6 个卖单，下方 6 个买单）。
  - `leverage`: 该交易对的杠杆倍数。

## 运行机器人

完成以上所有设置后，在激活了虚拟环境的终端中运行以下命令：

```bash
python grid_bot.py
```

程序启动后，您将在终端看到实时日志输出。所有日志也会被记录到 `grid_bot.log` 文件中。

## 停止机器人

在程序运行的终端中，按下 `Ctrl+C`。程序会捕获到这个信号，开始执行安全关闭流程，取消所有交易对的挂单，然后退出。 