# VCRF Refactor Blueprint

## 目标

把现有 agent 从“通用机会扫描器”升级成一套更符合以下融合哲学的系统：

- 巴菲特式 intrinsic value / owner mindset / margin of safety
- 周期研究（库存周期、资本开支传导、价格-利润-订单滞后）
- 困境反转（survival first, repair evidence second）
- 等风来（价值实现路径 + 边际买家 / marginal buyer）
- A 股优先，保留 U.S. adapter

---

## 一句话核心改造

不要把“鳄鱼 prompt”当主系统。

把它降级成 **cyclical adapter**；
把顶层系统改成：

**Value floor + Cycle/Repair inflection + Realization path + Flow confirmation**

---

## 顶层系统：四段式状态机

1. **Reject**
   - survival/gov/fraud/delisting 不能过
2. **Cold-storage / 埋伏库**
   - floor value 明确
   - price 低
   - flow 还没来
3. **Ready / 起风前**
   - cycle/repair 开始改善
   - catalyst 更具体
   - flow 仍弱，但出现早期异常
4. **Attack / 风来了**
   - 量价 + ownership + event 共振
   - 加仓而不是首次理解
5. **Harvest / 收割**
   - 估值到位 or crowding 过高 or thesis 被 price 吃完

---

## 建议保留的主类型（primary archetype）

仍保留：
- compounder
- cyclical
- turnaround
- asset_play
- special_situation

但增加正交修饰器（modifiers）：

- `cycle_state`: trough / repair / expansion / peak
- `repair_state`: none / stabilizing / repairing / confirmed
- `distress_source`: cyclical / operational / balance_sheet / governance / one_off
- `realization_path`: repricing / asset_unlock / M&A / buyback / policy / capital_return / institutional_entry
- `flow_stage`: abandoned / latent / ignition / trend / crowded
- `elasticity_bucket`: mega / large / mid / small / micro

重点：**不要再让 primary_type 独自决定估值与打分。**

---

## 文件级改造建议

### 1) sector_classification.yaml

从“单主类型分类”升级成“主类型 + 修饰器词库”。

新增：
- distress_sources
- realization_paths
- flow_keywords
- market_overlays
- country_specific_overrides

示意：

```yaml
realization_paths:
  capital_return:
    keywords: [回购, 特别分红, 增持]
  asset_unlock:
    keywords: [资产注入, 分拆上市, REIT, 出售资产, SOTP]
  industrial_consolidation:
    keywords: [并购重组, 吸收合并, 产业整合]
  cycle_repricing:
    keywords: [涨价, 去库存, 补库, capex, 景气回升]

market_overlays:
  A-share:
    preferred_elasticity_caps:
      cyclical: [5000000000, 80000000000]
      turnaround: [3000000000, 50000000000]
      asset_play: [8000000000, 120000000000]
  US:
    preferred_elasticity_caps:
      compounder: [1000000000, 30000000000]
      turnaround: [500000000, 10000000000]
```

---

### 2) scoring_rules.yaml

把静态八维改为“类型加权模板”。

建议八维：

- thesis_clarity: 5
- intrinsic_value_floor: 20
- survival_boundary: 15
- governance_anti_fraud: 10
- business_or_asset_quality: 10
- regime_cycle_position: 15
- turnaround_catalyst: 10
- flow_realization_and_elasticity: 15

同时增加 `weight_templates`：

```yaml
weight_templates:
  cyclical:
    intrinsic_value_floor: 20
    survival_boundary: 10
    governance_anti_fraud: 10
    business_or_asset_quality: 10
    regime_cycle_position: 25
    turnaround_catalyst: 10
    flow_realization_and_elasticity: 15
  turnaround:
    intrinsic_value_floor: 20
    survival_boundary: 20
    governance_anti_fraud: 15
    business_or_asset_quality: 5
    regime_cycle_position: 10
    turnaround_catalyst: 20
    flow_realization_and_elasticity: 10
```

---

### 3) valuation_discipline.yaml

把 bear/base/bull 升级成：

- `floor_case`
- `normalized_case`
- `recognition_case`

因为你真正要的是：

- **不来风时我亏多少？**
- **恢复常态值是多少？**
- **市场承认时能给多少？**

示意：

```yaml
opportunity_types:
  cyclical:
    methods:
      floor_case: tangible_book_or_replacement_cost
      normalized_case: mid_cycle_earnings
      recognition_case: rerated_mid_cycle_earnings
  turnaround:
    methods:
      floor_case: survival_value
      normalized_case: repaired_earnings
      recognition_case: post_repair_rerating
  asset_play:
    methods:
      floor_case: stressed_nav
      normalized_case: book_or_nav
      recognition_case: catalyst_discount_close
compatibility_defaults:
  cold_storage_min_floor_protection: 0.85
  cold_storage_min_normalized_upside: 0.40
  attack_min_normalized_upside: 0.25
  attack_min_flow_stage: ignition
```

---

### 4) framework_utils.py

新增函数：

- `determine_driver_stack(...)`
- `assess_intrinsic_value_floor(...)`
- `assess_normalized_earnings_power(...)`
- `assess_turnaround_evidence(...)`
- `assess_flow_realization(...)`
- `assess_elasticity(...)`
- `assess_distress_source(...)`
- `pick_weight_template(...)`

其中 `assess_flow_realization()` 是这次最关键的新增：

A 股可用：
- long-base breakout
- relative strength
- turnover expansion
- shareholder concentration
- northbound / fund ownership change
- buyback / dividend / M&A / asset injection signals

美股可用：
- Form 4 insider buying
- 13D / 13G beneficial owner events
- buyback authorization / execution
- 13F accumulation as confirmation
- earnings revisions / estimate changes

---

### 5) universal_gate.py

从“静态 gate + keyword heuristic”改成更接近 underwriting 的 six-gate：

1. `business_or_asset_truth`
2. `survival_truth`
3. `governance_truth`
4. `regime_cycle_truth`
5. `valuation_floor_truth`
6. `realization_truth`

其中第 6 门不只是 catalyst，必须变成：

```python
realization_truth = catalyst_quality + marginal_buyer_probability + flow_stage
```

#### 建议 hard veto

- 审计/财务造假/反复处罚/资金占用严重
- 净资产为负且无明确重整路径
- 现金流断裂 + 债务墙迫近 + 无国资/银行/产业资本兜底
- 解释不清 business truth
- 退市风险高但无明确化解路径

#### 建议 soft veto

- 价格已接近 recognition value
- A 股弹性策略里总市值/自由流通市值过大
- 量化拥挤/ETF重仓/高 consensus 大票，赔率不够

---

### 6) valuation_engine.py

建议新增输出：

```python
{
  "floor_case": ...,
  "normalized_case": ...,
  "recognition_case": ...,
  "summary": {
    "floor_protection": ...,
    "normalized_upside": ...,
    "recognition_upside": ...,
    "wind_dependency": ...,
    "priced_state": ...,
  }
}
```

#### 关键思想

- `floor_case` 负责“我为什么敢埋伏”
- `normalized_case` 负责“正常情况下值多少钱”
- `recognition_case` 负责“风来时市场可能给到哪”

---

### 7) radar_scan_engine.py

这份文件当前最大的结构问题：

- `_load_universe()` 按总市值降序拿前 `limit`
- `limit=24` 时，本质是在扫“24个大市值样本”
- 与“小市值弹性 + 等风来”的目标冲突

建议改为：

#### universe 构造

- 先全市场基础池
- 再按市值桶 / 行业 / 流动性 / 风险标记分层抽样
- A 股默认偏向：中小票，但保留少量中大票做资产重估或政策中军

#### 分层桶建议

- micro: 20-50亿
- small: 50-150亿
- mid: 150-500亿
- large: 500亿以上

#### A 股默认桶配比

- micro 25%
- small 40%
- mid 25%
- large 10%

#### Scanner 输出标签

- `Priority shortlist` = ready / attack
- `Secondary watchlist` = cold_storage
- `Reject / no-action` = reject

并在 payload 中加入：

```python
{
  "state": "cold_storage|ready|attack|harvest|reject",
  "flow_stage": "abandoned|latent|ignition|trend|crowded",
  "elasticity_bucket": "small",
  "floor_protection": 0.91,
  "normalized_upside": 0.58,
  "recognition_upside": 1.10,
}
```

---

## A 股专属 adapter

### 不该再做成硬闸门的旧规则

- 非央企一票否决
- PB <= 0.8 才允许研究
- 非三大同心圆就否决

### 该保留但降级为因子/修饰器的东西

- 国资属性 → survival / governance 加分
- PB 深折价 → floor value 加分
- 周期链条 → cyclical adapter 核心信号
- 小市值 → elasticity 加分

### A 股新增重点变量

- 自由流通市值（free-float cap）
- 筹码集中度 / 股东户数变化
- 长底后放量 / 20-60-120日强弱变化
- 产业资本增持 / 回购 / 分红提升
- 并购重组 / 资产注入 / REIT 事件
- 北向/公募/社保/QFII 持仓变化（能拿到就加）
- ST / ＊ST / 摘帽 / 重整的标签化处理

---

## 美股 adapter

不要把“主力拉升”照搬成美股语言。

美股里更好的 `marginal buyer` 代理：

- insider buying (Form 4)
- activist / control signals (Schedule 13D/13G)
- buyback
- 13F accumulation（确认，不是精确 timing）
- estimate revision / guidance reset

### 美股的评分侧重点

- quality/junk filter 更严格
- small cap 是乘数，不是前置门槛
- FCF / buyback / insider alignment 权重更大
- 主题情绪权重比 A 股低，资金披露权重更高

---

## 组合层（portfolio construction）

不要把所有仓位都用同一个规则分配。

建议三层：

1. `research inventory`
2. `cold-storage positions`
3. `attack positions`

### 建议加仓规则

- cold-storage 初始 2%-4%
- ready 级别 5%-8%
- attack 级别 8%-15%
- 高风险 special situation 单票上限低于普通 cyclical/asset play

### 建议减仓规则

- recognition value 达成 70%-100%
- flow_stage == crowded
- negative divergence（价升量缩 / 基本面跟不上）
- initial thesis 失真

---

## 伪代码

```python
if hard_veto(scan_data):
    return reject

stack = determine_driver_stack(scan_data, market=market)
value = assess_intrinsic_value_floor(scan_data, stack)
cycle = assess_regime_cycle(scan_data, stack)
repair = assess_turnaround_evidence(scan_data, stack)
flow = assess_flow_realization(scan_data, stack, market=market)
elasticity = assess_elasticity(scan_data)
valuation = build_floor_normalized_recognition_valuation(scan_data, stack)

state = classify_state(
    value_floor=valuation["summary"]["floor_protection"],
    normalized_upside=valuation["summary"]["normalized_upside"],
    repair_state=repair["state"],
    flow_stage=flow["stage"],
)

score = weighted_score(
    template=pick_weight_template(stack),
    value=value,
    cycle=cycle,
    repair=repair,
    flow=flow,
    elasticity=elasticity,
)

return {
    "stack": stack,
    "valuation": valuation,
    "state": state,
    "score": score,
}
```

---

## 最关键的判断

### 这套融合能不能优于鳄鱼原型？

能，但前提是：

- **鳄鱼逻辑只做 cyclical adapter，不做世界观**
- **把“价值实现者”单独建模**
- **把小市值看作收益放大器，而不是垃圾收容器**
- **把 turnaround 按“困境来源”分层，而不是一律当机会**

### 为什么更可能赚到更大钱？

因为它同时抓三种错配：

1. 价值错配（price << floor/normalized value）
2. 时间错配（cycle/repair 已变，价格未反映）
3. 认知错配（边际买家尚未全面入场）

大行情里，第三种错配最值钱；
而你现在系统最缺的，恰恰就是对第三种错配的算法化表达。

