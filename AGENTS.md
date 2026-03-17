# A股研究项目约定

## 系统定位

本项目已从旧的鳄鱼左侧狙击终端，迁移为两个边界清晰的独立 skill：

- `market-opportunity-scanner`
  负责全市场扫描、排序、shortlist、watchlist、no-action 判断。
- `single-stock-deep-dive`
  负责单票尽调、三情景估值、反证、行动框架。

旧目录 `a_stock_sniper/` 已归档到 `.agents/_archive/a_stock_sniper/`，不再位于 active skills 路径内。

## 命名约束

- `量化选股` 只指当前仓库 `A价投+周期`
- `点金术` 只指独立项目 `A点金术`
- `鳄鱼` 只指遗留项目 `A鳄鱼`

文档、脚本和迁移说明中禁止互相借名。

## 核心投资框架

### 五类机会类型

所有标的必须先做 opportunity typing，再做后续估值与判断：

- `compounder`
- `cyclical`
- `turnaround`
- `asset_play`
- `special_situation`

### 六道 universal gates

所有扫描和深挖都必须经过以下六道 gate：

1. Business truth
2. Survival truth
3. Quality truth
4. Regime / cycle truth
5. Valuation truth
6. Catalyst truth

### 八维评分

统一使用 100 分制：

- Type clarity: 5
- Business quality: 20
- Survival boundary: 15
- Management and capital allocation: 10
- Regime / cycle position: 15
- Valuation and margin of safety: 20
- Catalyst and value realization: 10
- Market structure and tradability: 5

评分分档：

- `85-100`: high conviction / strong candidate
- `75-84`: reasonable candidate / starter possible
- `65-74`: watchlist / incomplete edge
- `<65`: reject / no action

## 明确废除的旧纪律

以下逻辑不再作为系统硬约束：

- 三大同心圆能力圈限制
- 非央企 / 非省国资立即击杀
- `PB <= 0.8` 才允许研究
- `PB > 1.0` 物理封顶 84 分
- `eco_circle == unknown` 视为能力圈外
- “诚实空窗” 仅以破净与否触发

允许保留的旧资产仅限基础设施：

- akshare / cninfo / commodity / stats_gov adapters
- Tier 0 PDF 索引、autofill、verification 链路
- execution log 与 resume 骨架

## 目录约定

- `.agents/skills/market-opportunity-scanner/`
  新的市场扫描 skill
- `.agents/skills/single-stock-deep-dive/`
  新的单票深挖 skill
- `.agents/skills/shared/`
  共享 adapters、validators、utils、engines、config

核心共享配置：

- `.agents/skills/shared/config/scoring_rules.yaml`
- `.agents/skills/shared/config/valuation_discipline.yaml`
- `.agents/skills/shared/config/sector_classification.yaml`
- `.agents/skills/shared/config/moat_dictionary.yaml`
- `.agents/skills/shared/config/source_registry.yaml`

## 运行方式

### macOS bootstrap

```bash
./scripts/bootstrap_macos.sh
```

默认使用 Homebrew `python3.11` 创建本地 `.venv`，并统一通过 `.venv/bin/python` 运行。

### 市场扫描

```bash
.venv/bin/python scripts/run_quant_scan.py A-share --limit 24
```

也可直接传代码列表：

```bash
.venv/bin/python scripts/run_quant_scan.py 600328,600348,600893 --base-dir /tmp/a_quant_run
```

### 单票深挖

```bash
.venv/bin/python scripts/run_quant_deep_dive.py 600328 中盐化工
```

若只跑 Tier 1 与框架判断：

```bash
.venv/bin/python scripts/run_quant_deep_dive.py 600328 中盐化工 --skip-tier0 --base-dir /tmp/a_quant_run
```

`--base-dir` 优先级高于 `A_STOCK_BASE` 环境变量，高于仓库根目录。
当前不支持 `--resume`。

## 输出纪律

### Scanner

输出必须是以下三档之一：

- `Priority shortlist`
- `Secondary watchlist`
- `Reject / no-action`

如果样本中没有足够 edge，必须明确输出 `no-action`，而不是为了满足提示词强行凑名单。

### Deep Dive

报告必须覆盖：

- executive view
- market perception vs what market misses
- primary type
- six-gate conclusion
- bear / base / bull valuation
- anti-thesis
- falsification points
- action plan
- one-line bottom line

## 实现原则

- 先分类，再估值。
- 先判断是否值得研究，再展开叙事。
- 事实、推断、判断要分开。
- 估值方法必须随类型切换，禁止所有股票共用单一 PB / PE 纪律。
- 国资属性只作为 quality / governance 输入，不再是硬闸门。
- PB 只是 valuation gate 的一个输入，不再是机械否决器。
