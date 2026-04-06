# Stock Analysis Engine

An opinionated research sandbox for turning raw market data into actionable stock signals.

This project combines technical indicators, valuation screens, point-in-time-inspired fundamentals, sentiment scoring, and multi-factor ranking into a notebook-first workflow for stock research. It is built for fast iteration: inspect a single name, simulate decisions, then scale the same logic across entire indexes.

## Mission

Build a practical market intelligence engine that helps surface conviction faster than manual chart-reading, headline-scanning, and spreadsheet triage ever could.

## Vision

Evolve this codebase into a serious retail-to-pro research platform:

- A place where price action, business quality, valuation, and market narrative are scored in one system.
- A repeatable workflow for moving from idea generation to signal validation to portfolio action.
- A modular base for future automation, dashboards, backtests, portfolio overlays, and factor research.

## Why This Project Exists

Most stock research workflows are fragmented:

- Charts live in one app.
- Financial statements live in another.
- News and social chatter live somewhere else.
- Conviction ends up living in gut feel.

This codebase closes that gap. It pulls those inputs into one Python-driven pipeline and converts them into ranked outputs you can inspect, challenge, and improve.

## What It Does

### 1. Single-stock analysis

For an individual ticker, the project can:

- Fetch historical price data from Yahoo Finance.
- Compute technical indicators such as RSI, SMA, EMA, MACD, Bollinger Bands, ATR, VWAP, and stochastic oscillators.
- Score bullish and bearish technical conditions.
- Pull key valuation statistics and convert them into a fundamental score.
- Derive expanded financial quality, growth, risk, and balance-sheet metrics from statements.
- Optionally layer in sentiment from Reddit and recent news using OpenAI scoring.
- Blend everything into a final numeric signal and a human-readable label:
  `STRONG SELL`, `WEAK SELL`, `HOLD`, `WEAK BUY`, `STRONG BUY`.

### 2. Trading simulation

The simulation workflow tests what would have happened if you had followed the generated signals over time.

It tracks:

- buy and sell decisions,
- cash balance,
- units held,
- average cost basis,
- portfolio value,
- total profit and loss.

### 3. Index-wide screening

The index search workflow expands the same logic across major benchmarks and ranks constituents by signal strength.

Currently supported index universes:

- `dow30`
- `nasdaq100`
- `sp500`
- `ftse100`
- `ftse250`

The project also caches index constituents locally to reduce repeated scraping and speed up scans.

## Core Edge

This is not just an indicator notebook. The interesting part is the signal fusion:

- Technical analysis captures market behavior.
- Fundamental analysis captures valuation.
- Derived financial metrics capture quality, growth, and strength.
- Sentiment analysis captures narrative pressure.
- A multi-factor model turns those dimensions into a unified ranking score.

That structure makes the project much more extensible than a one-off notebook script.

## Research Philosophy

The codebase is built around a simple idea:

> Strong stock decisions should not depend on one lens.

Instead of betting everything on chart setups or valuation alone, this engine asks:

- Is the price trend constructive?
- Is the company cheap, expensive, or fairly priced?
- Is the business improving in quality and growth?
- Is the balance sheet resilient?
- Is the market narrative supportive or deteriorating?

When those signals align, the result should be more meaningful than any single factor on its own.

## Architecture

```text
stock analysis/
|-- 01_specific_stock_minima_maxima.ipynb
|-- 02_specific_stock_simulation.ipynb
|-- 03_index_search.ipynb
|-- analysis_functions/
|   |-- technical_analysis.py
|   |-- fundamental_analysis.py
|   |-- sentiment_analysis.py
|   |-- multifactor_analysis.py
|-- analysis_interfaces/
|   |-- interface_specific_stock.py
|   |-- interface_index_search.py
|-- analysis_types/
|   |-- prediction.py
|   |-- simulation.py
|-- dao/
|   |-- dao.py
|-- config/
|   |-- config.py
|   |-- example.config.ini
|   |-- logging_config.py
|-- cache/
|-- output/
|-- portfolio/
```

## Module Breakdown

### `dao/`

The data access layer. It is responsible for:

- Yahoo Finance price history
- Yahoo Finance key stats
- advanced statement-derived financial inputs
- Google News RSS ingestion
- Reddit post collection
- OpenAI sentiment scoring requests
- index constituent collection and caching

### `analysis_functions/`

The core analytics engine.

- `technical_analysis.py`: momentum, trend, volatility, and volume indicators
- `fundamental_analysis.py`: valuation, quality, growth, and financial-strength scoring
- `sentiment_analysis.py`: Reddit/news ingestion and sentiment aggregation
- `multifactor_analysis.py`: composite factor model with weighted ranking outputs

### `analysis_types/`

The transformation layer between raw analytics and usable outputs.

- `prediction.py`: builds the end-to-end prediction frame and final signal
- `simulation.py`: converts signals into trading simulations and portfolio traces

### `analysis_interfaces/`

The notebook-facing orchestration layer.

- `interface_specific_stock.py`: single ticker analysis and simulation helpers
- `interface_index_search.py`: index scan and ranking workflows

## Notebook Workflows

### `01_specific_stock_minima_maxima.ipynb`

Focused single-name analysis. The notebook currently shows a quick research workflow around a chosen ticker, with a short-period setup suited for recent signal inspection.

Example parameters already present in the notebook:

- `ticker = "NVDA"`
- `initial_funds = 100`
- `include_sentiment = False`
- `period = "5d"`

### `02_specific_stock_simulation.ipynb`

Single-stock strategy simulation over a longer window.

Example parameters already present in the notebook:

- `ticker = "TSLA"`
- `initial_funds = 100`
- `include_sentiment = False`
- `period = "1y"`

### `03_index_search.ipynb`

Batch scans multiple indexes and ranks names by signal strength.

Example parameters already present in the notebook:

- `index_names = ["dow30", "nasdaq100", "sp500", "ftse100", "ftse250"]`
- per-index limits via `limits_by_index`
- `include_sentiment = False`
- `period = "1y"`

## Signal Engine

The final signal is built from four major components:

1. Technical score
2. Sentiment score
3. Fundamental valuation score
4. Multi-factor composite score

Those components are blended using configurable weights from `config/config.ini`.

The shipped example config exposes:

- technical weight
- sentiment weight
- fundamental weight
- multifactor weight
- strong/weak buy and sell thresholds
- technical indicator parameters
- cache configuration
- logging configuration

## Multi-Factor Model

The multi-factor layer is one of the strongest parts of the repository.

It currently incorporates:

- value
- quality
- momentum
- sentiment
- risk
- liquidity

Under the hood, that includes metrics such as:

- ROIC
- ROE
- gross and operating margins
- free cash flow margin
- debt ratios
- interest coverage
- EPS growth
- revenue growth
- EBITDA growth
- Piotroski F-Score
- Altman Z-Score
- 3/6/12 month returns
- historical volatility
- maximum drawdown
- Sharpe ratio
- Sortino ratio
- analyst recommendation score
- price target upside

## Point-in-Time Thinking

This project goes beyond naive “latest fundamentals everywhere” logic.

The prediction pipeline includes a point-in-time-style financial snapshot process that:

- reads historical financial statement dates,
- applies availability lags to approximate when reports would have been known,
- maps market observations to the latest available fundamentals at that point in time.

That is a meaningful upgrade over simplistic backtests that leak future information into old decisions.

## Tech Stack

- Python
- Jupyter Notebook
- pandas
- numpy
- matplotlib
- yfinance
- requests
- BeautifulSoup (`bs4`)
- OpenAI Python SDK
- built-in logging with rotating file handlers

## Setup

### 1. Create a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

There is no pinned `requirements.txt` yet, so install the libraries used by the codebase directly:

```powershell
pip install pandas numpy matplotlib yfinance requests beautifulsoup4 openai lxml html5lib notebook
```

### 3. Configure the project

Use the example config as your template:

```powershell
Copy-Item config\example.config.ini config\config.ini
```

Then update `config/config.ini` with your local settings, especially:

- `chatgpt_key`
- signal weights
- buy/sell thresholds
- cache and logging preferences

## How To Run

Launch Jupyter and open any of the three notebooks:

```powershell
jupyter notebook
```

Suggested progression:

1. Start with `01_specific_stock_minima_maxima.ipynb` to inspect a single ticker.
2. Move to `02_specific_stock_simulation.ipynb` to evaluate signal-driven trading behavior.
3. Use `03_index_search.ipynb` to scan broader universes and rank opportunities.

## Outputs and Operational Folders

- `cache/`: cached index constituent lists
- `logs/`: rotating application logs
- `output/`: exported workflow outputs such as signal tables
- `portfolio/`: local portfolio state and portfolio-related artifacts

## Configuration Surface

The configuration layer is intentionally broad, which makes the system easy to tune.

You can control:

- moving average windows
- RSI thresholds and period
- ATR thresholds and period
- Bollinger Band period
- stochastic oscillator parameters
- MACD fast/slow/signal periods
- signal component weights
- buy/sell cutoffs
- cache expiry
- log level and log location

## Why The Codebase Is Worth Building On

This repository already has the bones of a much bigger system:

- a modular analytics core,
- reusable workflow interfaces,
- a scoring framework instead of hard-coded one-off decisions,
- simulation support,
- index-level batch processing,
- local caching and logging,
- a clean path toward APIs, dashboards, backtests, and scheduled jobs.

In other words: this is not just a collection of notebooks. It is the early-stage operating system for a serious stock research platform.

## Roadmap Ideas

Natural next steps for the project:

- add a pinned `requirements.txt` or `pyproject.toml`
- add unit tests around signal math and factor scoring
- persist historical scan results for time-series comparison
- add benchmark-aware backtesting
- add portfolio allocation logic on top of single-name signals
- build a lightweight dashboard for ranked ideas and simulation summaries
- add scheduler support for recurring scans
- export richer CSV and JSON artifacts

## Important Notes

- This project is best described as a research and experimentation engine, not a production trading system.
- Sentiment analysis requires an OpenAI API key and adds latency and cost.
- External data quality depends on Yahoo Finance, Google News RSS, Reddit, and page parsing stability.
- Financial signals are heuristic and should be validated before capital is allocated.

## Disclaimer

This repository is for research, education, and experimentation. It is not financial advice, not a guarantee of performance, and not a substitute for proper risk management.

## Closing Thought

If you want a codebase that can move from “interesting stock idea” to “ranked, explainable, testable signal,” this one is already pointed in the right direction.
