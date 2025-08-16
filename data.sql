CREATE TABLE trades_minute AS
SELECT
  date_trunc('minute', exchange_ts) AS minute_ts,
  symbol,
  SUM(price * qty)::double precision / NULLIF(SUM(qty),0) AS vwap,
  AVG(price) AS price_avg,
  MAX(price) AS price_high,
  MIN(price) AS price_low,
  SUM(qty) AS volume,
  SUM(CASE WHEN is_block_trade THEN qty ELSE 0 END) AS block_qty,
  SUM(CASE WHEN is_block_trade THEN price*qty ELSE 0 END) AS block_turnover,
  COUNT(*) AS tick_count
FROM raw_trades
GROUP BY 1,2;
CREATE INDEX ON trades_minute(symbol, minute_ts);


-- Null timestamp / zero volume rows
SELECT COUNT(*) FROM raw_trades WHERE exchange_ts IS NULL OR qty IS NULL OR price IS NULL;

-- Distinct symbols / time range
SELECT MIN(exchange_ts), MAX(exchange_ts) FROM raw_trades;
SELECT symbol, COUNT(*) FROM raw_trades GROUP BY symbol ORDER BY 2 DESC LIMIT 20;


CREATE TABLE options_oi_minute AS
SELECT date_trunc('minute', minute_ts) AS minute_ts, symbol, strike, expiry,
       SUM(oi) AS oi -- or last_value(oi) using window if already cumulative
FROM options_oi
GROUP BY 1,2,3,4;
CREATE INDEX ON options_oi_minute(symbol, minute_ts);


SELECT minute_ts, symbol,
       SUM(block_qty)::double precision / NULLIF(SUM(qty),0) AS block_share
FROM (
  SELECT date_trunc('minute', exchange_ts) AS minute_ts, symbol, qty, is_block_trade
  FROM raw_trades
) t
GROUP BY 1,2
ORDER BY minute_ts DESC LIMIT 100;



CREATE TABLE labeled_minute AS
SELECT t.*,
       CASE WHEN EXISTS(
         SELECT 1 FROM sebi_events e
         WHERE t.minute_ts BETWEEN e.start_ts AND e.end_ts
       ) THEN 1 ELSE 0 END AS label
FROM trades_minute t;


COPY (SELECT * FROM labeled_minute WHERE symbol = 'BANKNIFTY') TO '/tmp/labeled_minute_BANKNIFTY.csv' CSV HEADER;
