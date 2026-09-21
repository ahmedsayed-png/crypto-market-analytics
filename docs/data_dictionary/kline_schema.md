# Binance Kline / Candlestick Data Dictionary

## Raw Binance REST API Kline Response Structure

Binance REST API Kline endpoints return arrays of values for each candlestick interval:

| Index | Raw Field Name              | Data Type | Description |
|-------|-----------------------------|-----------|-------------|
| 0     | `open_time`                 | Long      | Kline open time (Epoch milliseconds) |
| 1     | `open`                      | String    | Opening price |
| 2     | `high`                      | String    | Highest price |
| 3     | `low`                       | String    | Lowest price |
| 4     | `close`                     | String    | Closing price |
| 5     | `volume`                    | String    | Base asset volume |
| 6     | `close_time`                | Long      | Kline close time (Epoch milliseconds) |
| 7     | `quote_volume`              | String    | Quote asset volume |
| 8     | `number_of_trades`          | Integer   | Total number of trades during interval |
| 9     | `taker_buy_volume`          | String    | Taker buy base asset volume |
| 10    | `taker_buy_quote_volume`    | String    | Taker buy quote asset volume |
| 11    | `ignore`                    | String    | Unused field (dropped during ingestion) |

---

## Logical Normalized Schema

During ingestion and Spark transformation, data is normalized into the following target schema:

| Field Name               | Spark / SQL Data Type | Description |
|--------------------------|----------------------|-------------|
| `timestamp`              | `TIMESTAMP`          | Candlestick open time formatted as timestamp |
| `symbol`                 | `STRING`             | Cryptocurrency pair symbol (e.g., `BTCUSDT`) |
| `open`                   | `DOUBLE`             | Opening price |
| `high`                   | `DOUBLE`             | Highest price |
| `low`                    | `DOUBLE`             | Lowest price |
| `close`                  | `DOUBLE`             | Closing price |
| `volume`                 | `DOUBLE`             | Base asset trading volume |
| `quote_volume`           | `DOUBLE`             | Quote asset trading volume |
| `number_of_trades`     | `LONG`               | Total number of trades executed |
| `taker_buy_volume`       | `DOUBLE`             | Base asset volume bought by takers |
| `taker_buy_quote_volume` | `DOUBLE`             | Quote asset volume bought by takers |
