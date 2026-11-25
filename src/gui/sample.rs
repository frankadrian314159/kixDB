//! Sample market-tick workspace loaded via `Workspace > Load sample tick database`.

pub const SAMPLE_Q: &str = "\
instrument: ([sym: `AAPL`MSFT`GOOG`NVDA`AMZN]
  name: (\"Apple Inc\"; \"Microsoft Corp\"; \"Alphabet Inc\"; \"NVIDIA Corp\"; \"Amazon.com Inc\");
  sector: `Tech`Tech`Comm`Semi`Retail;
  lot: 100 100 50 50 100);

trade: ([]
  time: 09:30:00.100 09:30:01.250 09:30:04.500 09:31:12.000 09:31:45.300 09:32:10.000 09:35:02.100 09:35:40.000 09:36:15.000 09:38:00.000;
  sym: `AAPL`MSFT`NVDA`AAPL`GOOG`NVDA`MSFT`AAPL`AMZN`NVDA;
  price: 185.20 412.50 924.00 185.65 168.40 926.50 413.10 185.40 198.75 928.25;
  size: 200 100 50 400 150 100 300 250 100 150;
  exch: `N`Q`Q`N`Q`P`Q`N`Q`Q);

quote: ([]
  time: 09:30:00.000 09:30:01.000 09:30:04.000 09:31:10.000 09:31:40.000 09:32:00.000 09:35:00.000 09:36:00.000;
  sym: `AAPL`MSFT`NVDA`AAPL`GOOG`NVDA`MSFT`AMZN;
  bid: 185.15 412.40 923.80 185.60 168.30 926.20 413.00 198.60;
  ask: 185.25 412.55 924.10 185.70 168.50 926.60 413.20 198.80);

vwap_summary:: select trades: count i, vwap: size wavg price, volume: sum size, hi: max price, lo: min price by sym from trade;

ohlc5m: {[s] select open: first price, high: max price, low: min price, close: last price, vol: sum size by bar: 5 xbar time.minute from trade where sym = s};
";
