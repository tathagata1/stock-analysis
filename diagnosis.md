# Stock Analysis Codebase Diagnosis

Audit date: 2026-08-16  
Remediation pass: 2026-08-16  
Scope: Python modules, notebooks, configuration, dependency metadata, caches, external integrations, signal construction, both backtesting paths, tests, and CI.

## Executive diagnosis

The repository is now a tested stock-research application rather than an unguarded notebook prototype. The remediation pass corrected the main causal signal defects, made missing data explicit, separated signal coverage from signal direction, hardened the index scan and caches, added a lagged composite-signal backtester, clarified price-intent execution rules, and introduced packaging, tests, and CI.

The code is suitable for exploratory screening and controlled strategy research. It is still not production-trading software, and results should not be described as fully point-in-time or survivorship-bias-free. The remaining constraints mostly come from data provenance and market-model realism rather than the repaired arithmetic and control flow:

- fundamental availability dates are estimated from statement dates and lags, not sourced from authoritative filing timestamps;
- restatements returned by the upstream provider can still appear in historical snapshots;
- index scans use current constituents, so long-horizon universe tests retain survivorship bias;
- the strategy equity curve does not credit dividends, even though passive and benchmark comparisons use adjusted close;
- short simulations do not model borrow availability, borrow fees, recalls, or margin rules; and
- fixed, heuristic score thresholds have not been statistically calibrated by sector or validated out of sample.

Current classification: **a robust research sandbox with explicit data-quality states and testable execution assumptions, but not a point-in-time institutional backtesting system or live trading engine**.

## Remediation summary

The original audit contained 26 findings. The current disposition is 16 resolved, 7 materially improved with a documented residual limitation, and 3 open architectural/data limitations.

| # | Original finding | Status | Current state |
|---:|---|---|---|
| 1 | High-volume feature used future observations | Resolved | Volume baseline is a prior-session rolling mean using `shift(1)`; warm-up remains missing rather than false. |
| 2 | Historical fundamentals were not backtest-grade | Improved | Quarterly flows are TTM, annual/quarterly frequency is preserved, overlapping annual rows win, shares come from statements, and estimated availability lags are recorded. Actual filing timestamps and restatements remain unresolved. |
| 3 | Minimum holding exit priority | Policy updated | Minimum holding now gates fixed stops, trailing stops, targets, and time exits. |
| 4 | Backtester did not test composite signals | Resolved | `run_composite_signal_backtest` evaluates a historical prediction frame with every signal shifted one session and execution at the next open. |
| 5 | Current constituents create survivorship bias | Open | Membership is cached and refresh-safe, but it is still current membership. Historical constituent data is required for a full fix. |
| 6 | Missing inputs were treated as neutral | Resolved | Missing factors are excluded, weights are renormalized, coverage is reported, and low-coverage rows become `INSUFFICIENT DATA`. |
| 7 | Negative valuation/leverage ratios could be rewarded | Resolved | Non-meaningful negative valuation and leverage domains no longer receive bullish modifiers. |
| 8 | Scan history could be shorter than warm-up | Resolved | Notebook/interface defaults are one year and technical analysis enforces an explicit minimum-history gate. |
| 9 | ATR rule was not scale invariant | Resolved | ATR is expressed and compared as a percentage of price. |
| 10 | VWAP depended on the requested period origin | Resolved | The daily-bar feature is now a bounded rolling volume-weighted average. |
| 11 | Target upside was disconnected | Resolved | `Target Mean Price` is collected and flows into the analyst target-upside factor/reporting path. |
| 12 | External failures could resemble valid results | Improved | Index failures are isolated and returned, stale-cache state is exposed, and signal availability is explicit. Some provider helper fallbacks still return missing data because a scan should continue. |
| 13 | Entry-candle exits were silently suppressed | Resolved | `entry_bar_exit_policy` explicitly selects `defer`, `stop`, or `target`, and ambiguous bars are counted. |
| 14 | Fixed stops/targets changed after entry | Resolved | `level_update_mode` explicitly selects entry-fixed or dynamic levels; stops cannot widen by default. |
| 15 | Total-return comparison was incomplete | Improved | Passive ticker and benchmark curves use adjusted close. Strategy dividends are still not posted to cash. |
| 16 | Short-sale model was simplified | Open | Costs/slippage are modeled, but borrow, margin, recall, and locate constraints are not. |
| 17 | Performance statistics were too narrow | Resolved | Summary includes return, drawdown, volatility, Sharpe, Sortino, win rate, average trade/holding period, profit factor, commissions, turnover, and ambiguity count. |
| 18 | Historical prediction generation was quadratic | Improved | Index scans default to latest-only fundamental/multifactor work and reuse ticker analyses across overlapping indices. Explicit full-history generation is still computationally expensive. |
| 19 | One bad ticker could abort an index | Resolved | Each ticker is isolated; successful rows and a structured failures frame are both returned. |
| 20 | Cache behavior was inconsistent | Improved | Writes are atomic, stale data can be retained on refresh failure, empty refreshes do not overwrite useful membership, and open-ended price requests use a stable cache key. Locking, pruning, and schema versioning remain absent. |
| 21 | Sentiment was expensive and fragile | Improved | API credentials/model are configurable, article text is delimited as untrusted input, strict JSON Schema is requested, payloads are validated independently, and malformed items do not erase valid ones. Persistent response caching, retry policy, and rate limiting remain absent. |
| 22 | Configuration depended on the working directory | Resolved | Configuration resolves relative to its module, supports `STOCK_ANALYSIS_CONFIG`, validates ranges, and prefers `OPENAI_API_KEY`. |
| 23 | Dependencies were not reproducible | Improved | Runtime versions are pinned with a Python 3.11-compatible NumPy marker, and `pyproject.toml` declares bounded package requirements. There is not yet a hash-locked transitive dependency set. |
| 24 | No automated quality gates existed | Resolved | A 29-test pytest suite and Python 3.11/3.12 GitHub Actions workflow are present. |
| 25 | README and notebook state were out of sync | Resolved | README documents both workflows and caveats; notebook defaults match the modules and saved outputs were cleared. |
| 26 | Internal duplication increased maintenance cost | Open | Several safe-conversion and Yahoo statement helpers remain duplicated between `dao` and analysis modules. |

## Current repository map

| Area | Primary files | Responsibility |
|---|---|---|
| Interactive entry points | `03_index_search.ipynb`, `04_backtesting.ipynb` | Index screening and price-intent backtesting examples |
| Interfaces | `analysis_interfaces/` | Notebook-facing orchestration for one ticker or one/more indices |
| Prediction | `analysis_types/prediction.py` | Builds technical/fundamental/multifactor/sentiment columns and the covered composite signal |
| Analysis | `analysis_functions/` | Indicator, fundamental, sentiment, multifactor, ranking, chart, and export logic |
| External data | `dao/dao.py` | Yahoo Finance, Wikipedia, Google News/article retrieval, OpenAI sentiment, and index cache |
| Price-intent backtest | `backtesting/engine.py` | Channel/price-level strategy simulator and transaction accounting |
| Signal backtest | `backtesting/signal.py` | Lagged composite-signal strategy evaluator |
| Backtest support | `backtesting/data.py`, `workflow.py`, `reporting.py` | Price cache, universe/benchmark loading, result tables, charts, and exports |
| Configuration | `config/` | Validated INI/environment settings and rotating logging |
| Delivery | `pyproject.toml`, `requirements*.txt`, `.github/workflows/tests.yml` | Packaging, repeatable direct dependencies, test dependencies, and CI |
| Verification | `tests/` | Unit and integration-style regression coverage for repaired behavior |

Package `__init__.py` files were added so the project can be installed and imported from outside the repository directory. One naming concern remains: `analysis_types/prediction.py` performs orchestration and is not merely a type definition; a future move to a `services` or `pipelines` package would better communicate its role.

## Architecture and execution flows

### Single-ticker composite analysis

`build_prediction_and_stats(ticker)` is the public notebook-facing path:

```text
Yahoo price history + current key statistics
  -> technical indicators and causal technical scores
  -> estimated point-in-time financial snapshots
  -> value and advanced financial metrics
  -> fundamental and multifactor component scores
  -> optional latest news sentiment
  -> available-component weight normalization
  -> numeric Signal + Signal_Text + coverage/status
```

The price frame is sorted oldest-first while rolling calculations are performed and returned newest-first, so interface code treats row zero as the latest observation. The default single-stock period is one year.

There are two analysis modes:

- `historical_analysis=True` computes historical fundamental and multifactor rows for research use. This is the slower path and inherits the point-in-time caveats below.
- `historical_analysis=False` retains the technical history but computes expensive current fundamental/multifactor work only for the latest row. Index scans use this mode by default.

Sentiment is intentionally a latest-observation input. Historical rows and sentiment-disabled runs use missing sentiment, not an artificial neutral score.

### Multi-index scan

`run_multi_index_search_workflow` performs the following:

```text
index names
  -> current constituent cache/scrape for each index
  -> shared ticker-analysis cache across all requested indices
  -> per-ticker failure isolation
  -> latest covered signal rows
  -> per-index results + failure diagnostics + cache metadata
  -> ranking, candidate detail, charts, and combined CSV output
```

The shared analysis cache prevents a company present in multiple requested indices from being downloaded and analyzed repeatedly. `signal_only` accepts only actual buy/sell classifications; `INSUFFICIENT DATA` is not accidentally treated as a signal.

### Price-intent backtest

`run_price_intent_backtest` is a reusable channel/price-level simulator. Rolling entry/stop/target levels are built from prior bars. Orders use deterministic OHLC fill rules plus commissions and slippage.

Important explicit policies are:

- `entry_bar_exit_policy`: `defer`, `stop`, or `target` when the entry bar touches an exit level;
- `level_update_mode`: `entry` for fixed entry-time levels or `dynamic` for recomputed levels;
- `allow_stop_widening=False`: the default prevents an active stop moving against the position;
- `minimum_holding_days` blocks every normal exit until the threshold is reached; and
- trade, round-trip, equity, passive, benchmark, summary, and execution-assumption outputs are returned together.

### Composite-signal backtest

`run_composite_signal_backtest` consumes a historical prediction frame. It shifts signal decisions by one trading session and executes at the next open, which removes same-session signal/fill look-ahead. It supports long-only or long/short use, transaction costs, slippage, terminal liquidation, passive comparison, and an optional benchmark.

This new engine makes the signal model testable, but it does not repair the upstream historical-universe or filing-date limitations. Those inputs determine whether a particular research result is credible.

## Module-by-module diagnosis

### `config/config.py`

The module now resolves `config.ini` beside the package instead of assuming the process working directory. `STOCK_ANALYSIS_CONFIG` can select another file. The OpenAI key is read from `OPENAI_API_KEY` first, placeholder asterisks are treated as unset, and the sentiment model can be selected with `OPENAI_SENTIMENT_MODEL` or INI configuration.

Validation covers positive indicator windows, ordered signal thresholds, factor weights, cache ages, volume/VWAP settings, signal coverage, and other numeric bounds. Configuration errors therefore fail early with useful messages.

Residual concern: importing configuration still performs file loading and validation. A typed settings object passed into workflows would make tests and multiple runtime profiles cleaner.

### `dao/dao.py`

This remains the broadest and most coupled module. It provides multiple independent adapters:

- Yahoo price data and current statistics;
- Yahoo financial statements and estimated historical snapshots;
- Wikipedia index constituents;
- Google News RSS and article text extraction;
- OpenAI sentiment classification; and
- index-membership cache management.

The remediation normalized Yahoo's percentage-style debt-to-equity value to the ratio convention used by the scoring functions and connected target mean price. Network/cache paths now use atomic replacement and safe stale fallback behavior.

OpenAI calls instantiate a client with an explicit key rather than mutating process environment. Article text is delimited and declared untrusted, and the request uses a strict two-number JSON Schema. Downstream parsing checks score and confidence ranges independently for each article.

Residual concerns are adapter breadth, duplicated safe-access helpers, scrape fragility, no persistent article/sentiment cache, and no explicit retry/backoff/rate-limit layer.

### `analysis_functions/technical_analysis.py`

The main causal and scale defects are repaired:

- high volume compares the current bar with a rolling average of earlier volume only;
- rolling warm-up produces missing values;
- the volume-weighted price has a bounded rolling origin;
- ATR is represented as a price percentage and does not mutate the caller's input frame;
- buy/sell rules require their source values to be present; and
- `technical_data_available` plus the minimum-history gate prevents partially warmed indicators from being presented as complete evidence.

The feature named VWAP is a rolling daily-bar volume-weighted average, not an intraday session VWAP. That distinction should remain explicit in user-facing interpretation.

### `analysis_functions/fundamental_analysis.py`

Scoring helpers now normalize `None`, `NaN`, and `pd.NA` consistently. Missing metrics do not contribute a zero that looks like measured neutrality. Negative or economically non-meaningful valuation/leverage ratios no longer earn cheapness or low-debt rewards. Partial mappings are accepted without raising key errors.

The thresholds are still heuristic and absolute. Cross-sector comparisons can be distorted because sensible leverage, margin, valuation, and growth ranges vary materially by industry and market regime.

### `analysis_functions/multifactor_analysis.py`

The model distinguishes unavailable factors from neutral factors. It detects which internal factors have evidence, renormalizes weights across the available subset, and returns `factor_coverage` and `available_factors`. Ratio helpers enforce meaningful denominator domains for relevant return/leverage calculations.

The factor model remains hand-tuned. There is no recorded training procedure, calibration set, uncertainty estimate, sector neutralization, or evidence that the factor weights generalize out of sample.

### `analysis_functions/sentiment_analysis.py`

Valid article payloads are retained even when another payload is malformed. Scores and confidences are range checked and combined rather than discarding the entire batch. No-article or unavailable-API cases remain missing at the prediction layer.

Operational limitations include API cost, latency, provider/site blocking, duplicate articles, lack of durable caching, and no model-version evaluation dataset.

### `analysis_types/prediction.py`

This module now owns an explicit availability-aware data contract. It adds component coverage columns, renormalizes top-level weights over available evidence, and reports:

- `signal_coverage`: fraction of configured top-level weight represented by usable inputs;
- `analysis_status`: `NO_DATA`, `INSUFFICIENT_DATA`, `PARTIAL`, or `COMPLETE`;
- `Signal`: missing below the configured minimum coverage; and
- `Signal_Text`: `INSUFFICIENT DATA` when a directional label is not justified.

Snapshot metadata (`availability_basis`, statement frequency) is propagated so consumers can distinguish annual from quarterly-TTM inputs and recognize estimated availability.

This is a major integrity improvement: a neutral market view is no longer interchangeable with a failed/missing data pipeline.

### `analysis_interfaces/`

The single-stock interface defaults to enough history for indicator warm-up and rejects empty price data. The index interface uses latest-only expensive analysis by default, catches errors per ticker, exposes failures separately, carries coverage/status into summaries, and shares results across overlapping index memberships.

The scan remains sequential. That is conservative for rate-limited providers but can be slow for large universes. Any future concurrency should be bounded per upstream provider and paired with retry/rate-limit policy.

### `analysis_functions/index_search_reporting.py`

Reporting now carries analysis status and signal coverage, excludes insufficient rows from directional candidate selection, and uses the connected target mean price for target-upside context. Shared ticker analyses eliminate earlier duplicate-ticker result drift between indices.

Charts and CSVs remain presentation outputs, not audit trails. For reproducible research, future exports should also record configuration, dependency/code revision, data retrieval timestamps, and failure/cache metadata.

### `backtesting/data.py`

Cached price files are normalized on read. Open-ended requests use a stable `latest` key instead of producing daily filenames. Writes are atomic, corrupt fresh data triggers a refresh, and stale data can be used when a refresh fails.

Remaining cache engineering work includes file locking for concurrent writers, retention/pruning, payload schema versions, checksums, and a machine-readable data-source version.

### `backtesting/engine.py`

The engine now makes ambiguous or consequential execution rules parameters instead of hiding them in control flow. Minimum-hold exit gating, entry-bar policy, level updating, non-widening stops, adjusted passive comparison, and richer statistics all have regression coverage.

The simulation is deliberately daily-bar and deterministic. When a bar touches multiple prices, the selected policy is an assumption rather than recovered intraday path. It also omits dividends in strategy cash, taxes, borrow mechanics, margin interest, market impact, partial fills, volume constraints, halts, and corporate-action-specific order handling.

### `backtesting/signal.py`

This is the new causal evaluator for composite signals. It validates required columns and costs, shifts decisions, records entries/exits, closes residual exposure at the end, and returns stable result frames even when no trades occur.

It expects the caller to provide a credible historical prediction frame. It should not be treated as protection against biased fundamentals, current-only membership, data revisions, or overfitted thresholds.

### `backtesting/reporting.py` and `workflow.py`

Reporting uses adjusted close for passive/benchmark curves and exposes the expanded result set. Workflow functions centralize cached market and benchmark loading. Notebook 04 passes the newly explicit execution policies through to the engine.

## Data contracts and research validity

### Price history

Analysis and backtest paths expect a date-indexed frame with `Open`, `High`, `Low`, `Close`, and `Volume`; adjusted close is used where available for passive total-return comparisons. Rolling calculations sort chronologically before evaluation.

The strategy trades raw OHLC levels. Split behavior therefore depends on the upstream history normalization. Research spanning corporate actions should include dedicated split/dividend tests before relying on the result.

### Fundamental snapshots

The snapshot builder now:

- derives shares from report-specific statements rather than projecting today's shares backward;
- sums quarterly income/cash-flow metrics into trailing-twelve-month flows;
- compares prior periods within the same annual or quarterly frequency;
- prefers annual data when annual and quarterly records overlap for a fiscal period;
- applies an estimated 90-day availability lag; and
- labels rows with `availability_basis="estimated_lag"` and their statement frequency.

These changes prevent several concrete mixing/look-ahead errors, but the data is still not authoritative point-in-time data. Provider statements may reflect later restatements, and a fixed lag cannot reproduce each company's real filing timestamp. Backtests using these snapshots must be labeled **estimated point-in-time**.

### Missingness and coverage

Missing inputs remain `NaN`/`pd.NA` through the scoring path. Component and factor weights are renormalized only over usable evidence. The final configured coverage threshold determines whether the model is allowed to emit a direction.

Coverage measures availability, not accuracy. A row can be `COMPLETE` while the underlying data is stale, revised, or economically incomparable. Freshness/provenance is a separate concern.

### Index membership

The index cache preserves the last useful current membership through transient refresh failures and reports stale/failure metadata. It does not store membership history by effective date. Any historical universe study must supply an external constituent timeline, delisted securities, and symbol-change mapping.

## Newly identified improvement areas

The remediation work exposed additional limitations that were not separate findings in the original audit:

1. **Model calibration and validation:** signal weights and thresholds are heuristic. Add walk-forward evaluation, an untouched holdout period, parameter-sensitivity plots, turnover-aware comparisons, and multiple-testing controls before making performance claims.
2. **Sector/regime normalization:** absolute valuation, leverage, volatility, and margin bands are not equally meaningful across sectors or interest-rate regimes. Consider cross-sectional ranks within sector and date.
3. **Research provenance:** exported results do not yet form a reproducible run manifest. Record commit identifier, configuration fingerprint, retrieval times, provider/source, snapshot frequency, cache status, failures, and dependency environment.
4. **Corporate actions:** adjusted comparison curves and raw tradable OHLC can diverge around splits/dividends. Add explicit corporate-action fixtures and decide how cash distributions affect open strategy positions.
5. **Static quality checks:** pytest guards behavior, but no formatter, linter, type checker, dependency vulnerability scan, or notebook execution check runs in CI.
6. **Full dependency locking:** exact top-level pins improve repeatability, but a platform-aware lock with hashes is needed for deterministic transitive installs.
7. **External-adapter boundaries:** splitting Yahoo, index, news, article, and OpenAI clients into small adapters would reduce coupling and make retry/circuit-breaker behavior testable.
8. **Cache lifecycle:** atomic replacement protects individual writes, but concurrent locks, expiry policy, pruning, schema migrations, and corruption telemetry are still needed.
9. **Live-trading separation:** there is no broker integration, order state machine, reconciliation, portfolio-level risk budget, secrets service, or operational alerting. These should be a separate layer rather than incremental additions to the research engine.

## Tests and verification

The current automated suite contains 29 tests covering:

- causal high-volume calculation, indicator warm-up, and short-history behavior;
- valuation/leverage domain handling;
- missing sentiment, component renormalization, signal coverage, and empty input;
- quarterly-TTM snapshots, overlap/frequency selection, estimated lag, and historical shares;
- target-price mapping and debt-to-equity normalization;
- malformed sentiment payload isolation and strict OpenAI JSON Schema requests;
- ticker failure isolation and duplicate analysis reuse;
- stale index-cache fallback;
- minimum-hold exit gating, entry-bar policy, stop widening, and adjusted passive return;
- one-session lag/no same-session trade in the composite-signal backtester; and
- configuration/import behavior outside the repository working directory.

Verification completed during this pass:

```text
python -m pytest
29 passed

python -m pip install -e . --no-deps
editable package installation succeeded

python -m compileall ...
all project modules compiled
```

Both notebooks parse as valid JSON and contain no saved execution outputs or execution counts. A live latest-only AAPL smoke test also completed with a one-year frame, a covered latest signal, a partial-data status, quarterly-TTM snapshot metadata, and connected target-price data. The smoke test verifies integration availability, not investment correctness.

## Recommended next sequence

### 1. Establish research-grade datasets

- acquire actual filing/availability timestamps and preserve as-reported versions;
- obtain historical index membership including deletions and symbol changes;
- define and test a corporate-action/dividend treatment; and
- store immutable raw-data snapshots with source/version metadata.

### 2. Validate the composite model

- run the lagged signal backtester over credible point-in-time inputs;
- use walk-forward and holdout periods;
- compare against adjusted passive and simple factor baselines;
- test costs, lag, threshold, weight, and missingness sensitivity; and
- report results by sector, regime, coverage band, and turnover.

### 3. Harden external operations

- split provider adapters;
- add bounded retries, rate limiting, durable sentiment/article caching, and request telemetry;
- add cache locks, schemas, retention, and run manifests; and
- preserve typed failure categories rather than only log text.

### 4. Raise delivery quality

- add formatting/linting/type checks and notebook smoke execution to CI;
- adopt a hash-locked dependency workflow;
- add coverage reporting and tests for corporate actions/no-trade/short edge cases; and
- document semantic versioning for exported result schemas.

## Bottom line

The original highest-risk implementation defects have been repaired and protected by regression tests. Causal technical features, explicit missingness, coverage-aware aggregation, resilient scans/caches, clear backtest policies, and a genuinely lagged signal evaluator materially improve the reliability of the repository.

The next quality boundary is data, not another scoring rule: actual point-in-time filings, historical constituents, corporate-action accounting, and disciplined out-of-sample validation are required before the system can support strong historical-performance claims. Until then, outputs should be presented as research signals with visible coverage and provenance caveats, not as production trading recommendations.
