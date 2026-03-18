import sys
import tempfile
import json
import unittest
import importlib.util
from pathlib import Path

import pandas as pd


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

from engines.backtest_engine import run_vcrf_backtest, select_round_candidates
from engines.flow_realization_engine import score_elasticity, score_flow_confirmation, score_realization_axis
from engines.signal_library_engine import expand_signal_daily, normalize_signal_month_end, resolve_effective_date
from engines.valuation_engine import build_three_case_valuation
from utils.config_loader import load_backtest_protocol
from utils.primary_type_router import build_driver_stack
from utils.vcrf_probes import assess_survival_boundary


class SurvivalProbeUpgradeTests(unittest.TestCase):
    def test_survival_probe_sets_tripwire_for_low_cash_coverage_without_state_support(self) -> None:
        result = assess_survival_boundary(
            {
                "company_profile": {"data": {"实际控制人": "自然人"}},
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 20_000_000}]},
                "balance_sheet": {
                    "data": [
                        {
                            "报告期": "20241231",
                            "资产总计": 2_000_000_000,
                            "归属于母公司所有者权益合计": 200_000_000,
                            "货币资金": 200_000_000,
                            "交易性金融资产": 100_000_000,
                            "短期借款": 300_000_000,
                            "一年内到期的非流动负债": 200_000_000,
                        }
                    ]
                },
                "cashflow_statement": {"data": [{"报告期": "20241231", "经营活动产生的现金流量净额": 50_000_000}]},
            }
        )

        self.assertAlmostEqual(result["cash_coverage"], 0.60, places=2)
        self.assertAlmostEqual(result["cfo_support"], 0.10, places=2)
        self.assertTrue(result["tripwire_reject"])
        self.assertLess(result["score"], 40.0)

    def test_survival_probe_uses_lower_tripwire_for_cyclical_non_soe_names(self) -> None:
        result = assess_survival_boundary(
            {
                "company_profile": {"data": {"实际控制人": "自然人"}},
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 20_000_000}]},
                "balance_sheet": {
                    "data": [
                        {
                            "报告期": "20241231",
                            "资产总计": 2_000_000_000,
                            "归属于母公司所有者权益合计": 200_000_000,
                            "货币资金": 200_000_000,
                            "交易性金融资产": 0,
                            "短期借款": 300_000_000,
                            "一年内到期的非流动负债": 200_000_000,
                        }
                    ]
                },
                "cashflow_statement": {"data": [{"报告期": "20241231", "经营活动产生的现金流量净额": -20_000_000}]},
            },
            {"primary_type": "cyclical", "sector_route": "core_resource"},
        )

        self.assertAlmostEqual(result["cash_coverage"], 0.40, places=2)
        self.assertAlmostEqual(result["tripwire_threshold"], 0.30, places=2)
        self.assertFalse(result["tripwire_reject"])

    def test_survival_probe_marks_no_debt_wall_with_liquid_assets_as_full_availability(self) -> None:
        result = assess_survival_boundary(
            {
                "company_profile": {"data": {"实际控制人": "自然人"}},
                "income_statement": {
                    "data": [
                        {
                            "报告期": "20241231",
                            "归属于母公司所有者的净利润": 20_000_000,
                            "利润总额": 40_000_000,
                            "财务费用": 20_000_000,
                        }
                    ]
                },
                "balance_sheet": {
                    "data": [
                        {
                            "报告期": "20241231",
                            "资产总计": 2_000_000_000,
                            "归属于母公司所有者权益合计": 500_000_000,
                            "交易性金融资产": 300_000_000,
                        }
                    ]
                },
                "cashflow_statement": {"data": [{"报告期": "20241231", "经营活动产生的现金流量净额": 50_000_000}]},
            }
        )

        self.assertEqual(result["availability"], "full")
        self.assertAlmostEqual(result["cash_coverage"], 2.0, places=2)
        self.assertFalse(result["tripwire_reject"])


class FlowAndElasticityUpgradeTests(unittest.TestCase):
    def test_elasticity_uses_continuous_free_float_cap_curve(self) -> None:
        micro = score_elasticity(
            {
                "realtime_quote": {"data": {"流通市值": 4_000_000_000}},
                "stock_kline": {"data": {"avg_turnover_20d": 30_000_000}},
            },
            {"modifiers": {"flow_stage": "latent"}},
        )
        mid = score_elasticity(
            {
                "realtime_quote": {"data": {"流通市值": 12_000_000_000}},
                "stock_kline": {"data": {"avg_turnover_20d": 30_000_000}},
            },
            {"modifiers": {"flow_stage": "latent"}},
        )
        large = score_elasticity(
            {
                "realtime_quote": {"data": {"流通市值": 35_000_000_000}},
                "stock_kline": {"data": {"avg_turnover_20d": 30_000_000}},
            },
            {"modifiers": {"flow_stage": "latent"}},
        )
        trapped = score_elasticity(
            {
                "realtime_quote": {"data": {"流通市值": 4_000_000_000}},
                "stock_kline": {"data": {"avg_turnover_20d": 10_000_000}},
            },
            {"modifiers": {"flow_stage": "latent"}},
        )

        self.assertGreater(micro["score"], 90.0)
        self.assertGreater(mid["score"], 45.0)
        self.assertLess(mid["score"], 80.0)
        self.assertLessEqual(large["score"], 20.0)
        self.assertLessEqual(trapped["score"], 15.0)

    def test_flow_confirmation_detects_left_side_absorption_pulses(self) -> None:
        result = score_flow_confirmation(
            {
                "stock_kline": {
                    "data": {
                        "drawdown_from_5yr_high_pct": 58.0,
                        "pulse_volume_events_30d": 2,
                        "avg_turnover_20d": 80_000_000,
                        "avg_turnover_120d": 32_000_000,
                        "volume_ratio_20_vs_120": 1.2,
                        "latest_close": 10.0,
                        "low_5y": 8.0,
                    }
                },
                "event_signals": {},
            },
            {"market": "A-share", "modifiers": {"flow_stage": "latent"}},
        )

        self.assertGreaterEqual(result["score"], 90.0)
        self.assertEqual(result["flow_stage"], "trend")

    def test_realization_axis_reweights_cyclical_scores_away_from_repair_and_default_50s(self) -> None:
        result = score_realization_axis(
            {
                "realtime_quote": {"data": {"流通市值": 60_000_000_000}},
                "stock_kline": {
                    "data": {
                        "current_vs_5yr_high": 45.0,
                        "drawdown_from_5yr_high_pct": 55.0,
                        "pulse_volume_events_30d": 2,
                        "avg_turnover_20d": 800_000_000,
                        "avg_turnover_120d": 300_000_000,
                        "volume_ratio_20_vs_120": 1.4,
                        "avg_turnover_1y": 700_000_000,
                        "latest_close": 10.0,
                        "low_5y": 7.0,
                    }
                },
                "event_signals": {},
            },
            {
                "market": "A-share",
                "primary_type": "cyclical",
                "sector_route": "core_resource",
                "modifiers": {
                    "repair_state": "none",
                    "cycle_state": "trough",
                    "elasticity_bucket": "large",
                    "realization_path": "repricing",
                    "flow_stage": "latent",
                },
            },
        )

        self.assertGreater(result["score"], 70.0)
        self.assertEqual(result["weights_used"]["repair_state"], 0.0)
        self.assertEqual(result["weights_used"]["marginal_buyer_probability"], 0.0)
        self.assertEqual(result["weights_used"]["catalyst_quality"], 0.0)
        self.assertGreater(result["weights_used"]["regime_cycle_position"], 0.45)
        self.assertGreater(result["weights_used"]["flow_confirmation"], 0.30)

    def test_elasticity_softens_large_liquid_cyclical_penalty(self) -> None:
        result = score_elasticity(
            {
                "realtime_quote": {"data": {"流通市值": 60_000_000_000}},
                "stock_kline": {"data": {"avg_turnover_20d": 800_000_000}},
            },
            {"primary_type": "cyclical", "modifiers": {"flow_stage": "latent", "elasticity_bucket": "large"}},
        )

        self.assertGreaterEqual(result["score"], 45.0)


class ValuationNormalizationTests(unittest.TestCase):
    def test_turnaround_normalized_case_uses_trimmed_margin_when_current_profit_is_negative(self) -> None:
        scan_data = {
            "realtime_quote": {"data": {"最新价": 5.0, "总市值": 5_000_000_000}},
            "stock_kline": {"data": {"latest_close": 5.0}},
            "income_statement": {
                "data": [
                    {"报告期": "20241231", "营业总收入": 18_000_000_000, "归属于母公司所有者的净利润": -300_000_000},
                    {"报告期": "20231231", "营业总收入": 15_000_000_000, "归属于母公司所有者的净利润": 900_000_000},
                    {"报告期": "20221231", "营业总收入": 13_000_000_000, "归属于母公司所有者的净利润": 1_300_000_000},
                    {"报告期": "20211231", "营业总收入": 11_000_000_000, "归属于母公司所有者的净利润": 880_000_000},
                    {"报告期": "20201231", "营业总收入": 12_000_000_000, "归属于母公司所有者的净利润": -1_200_000_000},
                    {"报告期": "20191231", "营业总收入": 10_000_000_000, "归属于母公司所有者的净利润": 2_000_000_000},
                ]
            },
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 4_000_000_000}]},
        }

        result = build_three_case_valuation(
            "600348",
            scan_data,
            {"primary_type": "turnaround", "sector_route": "core_resource"},
        )

        self.assertGreater(result["normalized_case"]["implied_price"], 0.0)
        self.assertEqual(result["summary"]["normalized_profit_source"], "trimmed_margin_x_latest_revenue")

    def test_turnaround_recognition_case_reuses_normalized_profit_when_current_profit_is_negative(self) -> None:
        scan_data = {
            "realtime_quote": {"data": {"最新价": 5.0, "总市值": 5_000_000_000}},
            "stock_kline": {"data": {"latest_close": 5.0}},
            "income_statement": {
                "data": [
                    {"报告期": "20241231", "营业总收入": 18_000_000_000, "归属于母公司所有者的净利润": -300_000_000},
                    {"报告期": "20231231", "营业总收入": 15_000_000_000, "归属于母公司所有者的净利润": 900_000_000},
                    {"报告期": "20221231", "营业总收入": 13_000_000_000, "归属于母公司所有者的净利润": 1_300_000_000},
                    {"报告期": "20211231", "营业总收入": 11_000_000_000, "归属于母公司所有者的净利润": 880_000_000},
                    {"报告期": "20201231", "营业总收入": 12_000_000_000, "归属于母公司所有者的净利润": -1_200_000_000},
                    {"报告期": "20191231", "营业总收入": 10_000_000_000, "归属于母公司所有者的净利润": 2_000_000_000},
                ]
            },
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 4_000_000_000}]},
        }

        result = build_three_case_valuation(
            "600348",
            scan_data,
            {"primary_type": "turnaround", "sector_route": "core_resource"},
        )

        self.assertGreater(result["recognition_case"]["implied_price"], 0.0)
        self.assertGreater(result["recognition_case"]["implied_price"], result["normalized_case"]["implied_price"])


class BigBathIntegrationTests(unittest.TestCase):
    def test_driver_stack_derives_big_bath_from_financial_statements(self) -> None:
        scan_data = {
            "company_profile": {"data": {"主营业务": "煤化工", "经营范围": "纯碱、烧碱", "行业": "化工"}},
            "revenue_breakdown": {
                "data": [
                    {"报告期": "20241231", "主营构成": "纯碱", "主营收入": 100.0, "主营利润": 30.0, "毛利率": 0.30},
                    {"报告期": "20231231", "主营构成": "纯碱", "主营收入": 100.0, "主营利润": 20.0, "毛利率": 0.20},
                ]
            },
            "income_statement": {
                "data": [
                    {
                        "报告期": "20241231",
                        "营业总收入": 18_000_000_000,
                        "归属于母公司所有者的净利润": -500_000_000,
                        "资产减值损失": -300_000_000,
                        "信用减值损失": -150_000_000,
                    },
                    {"报告期": "20231231", "营业总收入": 17_000_000_000, "归属于母公司所有者的净利润": -700_000_000},
                ]
            },
            "cashflow_statement": {"data": [{"报告期": "20241231", "经营活动产生的现金流量净额": 200_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 4_000_000_000}]},
            "realtime_quote": {"data": {"总市值": 5_000_000_000, "最新价": 5.0, "流通市值": 4_000_000_000}},
            "stock_kline": {"data": {"latest_close": 5.0, "current_vs_5yr_high": 45.0, "avg_turnover_20d": 30_000_000}},
            "valuation_history": {"data": {"pb": 0.8, "pb_percentile": 10.0}},
        }

        driver_stack = build_driver_stack("600348", scan_data)

        self.assertEqual(driver_stack["big_bath_result"]["verdict"], "big_bath")
        self.assertEqual(driver_stack["primary_type"], "turnaround")


class SignalLibraryTests(unittest.TestCase):
    def test_backtest_protocol_loader_exposes_round_defaults(self) -> None:
        protocol = load_backtest_protocol()
        self.assertEqual(protocol["round_size"], 3)
        self.assertEqual(protocol["total_rounds"], 10)

    def test_resolve_effective_date_uses_next_trading_day(self) -> None:
        trading_days = pd.DatetimeIndex(["2020-02-03", "2020-02-04", "2020-02-05"])
        effective_date = resolve_effective_date("2020-02-01", trading_days)
        self.assertEqual(str(effective_date.date()), "2020-02-03")

    def test_expand_signal_daily_forward_fills_until_next_effective_date(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {
                        "signal_date": "2020-01-31",
                        "effective_date": "2020-02-03",
                        "ticker": "600001",
                        "vcrf_state": "ATTACK",
                        "floor_price": 8.0,
                        "recognition_price": 12.0,
                        "total_score": 88.0,
                        "tradable_flag": 1,
                    },
                    {
                        "signal_date": "2020-02-29",
                        "effective_date": "2020-03-02",
                        "ticker": "600001",
                        "vcrf_state": "REJECT",
                        "floor_price": 7.0,
                        "recognition_price": 11.0,
                        "total_score": 20.0,
                        "tradable_flag": 1,
                    },
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04", "2020-02-05", "2020-03-02", "2020-03-03"]),
        )
        daily_bars = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-02-03", "2020-02-04", "2020-02-05", "2020-03-02", "2020-03-03"]),
                "ticker": ["600001"] * 5,
                "open": [10, 10, 10, 9, 9],
                "high": [10, 10, 10, 9, 9],
                "low": [10, 10, 10, 9, 9],
                "close": [10, 10, 10, 9, 9],
            }
        )

        signal_daily = expand_signal_daily(month_end, daily_bars)

        self.assertEqual(signal_daily.loc[signal_daily["date"] == pd.Timestamp("2020-02-05"), "vcrf_state"].iloc[0], "ATTACK")
        self.assertEqual(signal_daily.loc[signal_daily["date"] == pd.Timestamp("2020-03-02"), "vcrf_state"].iloc[0], "REJECT")


class RoundProtocolBacktestTests(unittest.TestCase):
    def test_backtest_csv_writer_serializes_summary_objects_as_json(self) -> None:
        repo_root = Path(__file__).resolve().parents[4]
        script_path = repo_root / "scripts" / "run_vcrf_backtest.py"
        spec = importlib.util.spec_from_file_location("run_vcrf_backtest_script_test", script_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)

        frame = pd.DataFrame(
            [
                {
                    "round_id": 1,
                    "exit_reason_counts": {"state_reject": 1, "target_hit": 2},
                    "candidate_diagnostics": [{"ticker": "AAA", "underwrite_score": 80.0}],
                    "anomalies": [],
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "round_summary.csv"
            module._write_frame(frame, out_path)
            written = pd.read_csv(out_path)

        self.assertEqual(json.loads(written.loc[0, "exit_reason_counts"]), {"state_reject": 1, "target_hit": 2})
        self.assertEqual(json.loads(written.loc[0, "candidate_diagnostics"]), [{"ticker": "AAA", "underwrite_score": 80.0}])
        self.assertEqual(json.loads(written.loc[0, "anomalies"]), [])

    def test_round_selection_only_keeps_attack_candidates(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 95.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "HARVEST", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 99.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "CCC", "vcrf_state": "READY", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 97.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03"]),
        )

        selected = select_round_candidates(month_end)

        self.assertEqual(selected["ticker"].tolist(), ["AAA"])

    def test_round_selection_excludes_used_tickers_across_rounds(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 95.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 90.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "CCC", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 85.0, "tradable_flag": 1},
                    {"signal_date": "2020-02-29", "effective_date": "2020-03-02", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 99.0, "tradable_flag": 1},
                    {"signal_date": "2020-02-29", "effective_date": "2020-03-02", "ticker": "DDD", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 88.0, "tradable_flag": 1},
                    {"signal_date": "2020-02-29", "effective_date": "2020-03-02", "ticker": "EEE", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 87.0, "tradable_flag": 1},
                    {"signal_date": "2020-02-29", "effective_date": "2020-03-02", "ticker": "FFF", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 86.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-03-02"]),
        )

        selected = select_round_candidates(
            month_end,
            {
                "initial_cash": 1_000_000,
                "round_size": 3,
                "total_rounds": 2,
                "exclude_used_tickers_across_rounds": True,
            },
        )

        self.assertEqual(selected[selected["round_id"] == 1]["ticker"].tolist(), ["AAA", "BBB", "CCC"])
        self.assertEqual(selected[selected["round_id"] == 2]["ticker"].tolist(), ["DDD", "EEE", "FFF"])

    def test_backtest_returns_empty_rounds_when_no_attack_candidates_exist(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "REJECT", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 10.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "HARVEST", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 99.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04"]),
        )
        daily_bars = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-02-03", "2020-02-04", "2020-02-03", "2020-02-04"]),
                "ticker": ["AAA", "AAA", "BBB", "BBB"],
                "open": [10, 10, 10, 10],
                "high": [10, 10, 10, 10],
                "low": [10, 10, 10, 10],
                "close": [10, 10, 10, 10],
            }
        )

        result = run_vcrf_backtest(month_end, daily_bars)

        self.assertTrue(result["selected_candidates"].empty)
        self.assertEqual(result["rounds"], [])
        self.assertTrue(result["summary"].empty)

    def test_backtest_uses_lot_rounding_limit_stop_and_reject_exit(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 8.0, "recognition_price": 12.0, "total_score": 95.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "ATTACK", "floor_price": 16.0, "recognition_price": 24.0, "total_score": 90.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "CCC", "vcrf_state": "ATTACK", "floor_price": 25.0, "recognition_price": 40.0, "total_score": 85.0, "tradable_flag": 1},
                    {"signal_date": "2020-02-29", "effective_date": "2020-02-05", "ticker": "CCC", "vcrf_state": "REJECT", "floor_price": 25.0, "recognition_price": 40.0, "total_score": 10.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04", "2020-02-05"]),
        )
        daily_bars = pd.DataFrame(
            [
                {"date": "2020-02-03", "ticker": "AAA", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2},
                {"date": "2020-02-04", "ticker": "AAA", "open": 10.3, "high": 12.5, "low": 10.1, "close": 12.0},
                {"date": "2020-02-05", "ticker": "AAA", "open": 12.1, "high": 12.2, "low": 11.9, "close": 12.0},
                {"date": "2020-02-03", "ticker": "BBB", "open": 20.0, "high": 20.2, "low": 19.5, "close": 19.8},
                {"date": "2020-02-04", "ticker": "BBB", "open": 19.7, "high": 19.9, "low": 15.5, "close": 16.2},
                {"date": "2020-02-05", "ticker": "BBB", "open": 16.1, "high": 16.3, "low": 16.0, "close": 16.1},
                {"date": "2020-02-03", "ticker": "CCC", "open": 30.0, "high": 30.5, "low": 29.8, "close": 30.2},
                {"date": "2020-02-04", "ticker": "CCC", "open": 30.4, "high": 31.0, "low": 30.0, "close": 30.8},
                {"date": "2020-02-05", "ticker": "CCC", "open": 32.0, "high": 32.2, "low": 31.5, "close": 31.8},
            ]
        )

        result = run_vcrf_backtest(
            month_end,
            daily_bars,
            protocol={
                "initial_cash": 1_000_000,
                "round_size": 3,
                "total_rounds": 1,
                "exclude_used_tickers_across_rounds": True,
                "lot_size": 100,
                "max_holding_bars": 504,
                "same_bar_conflict": "stop_first",
                "costs": {
                    "broker_commission_bps": 0.0,
                    "broker_min_commission": 0.0,
                    "transfer_fee_bps": 0.0,
                    "slippage_bps_buy": 0.0,
                    "slippage_bps_sell": 0.0,
                    "stamp_duty": [],
                },
            },
        )

        trades = result["rounds"][0]["trades"]
        self.assertEqual(trades["ticker"].tolist(), ["AAA", "BBB", "CCC"])
        self.assertTrue(all(trades["shares"] % 100 == 0))
        self.assertEqual(trades.set_index("ticker").loc["AAA", "exit_reason"], "target_hit")
        self.assertEqual(trades.set_index("ticker").loc["BBB", "exit_reason"], "floor_stop")
        self.assertEqual(trades.set_index("ticker").loc["CCC", "exit_reason"], "state_reject")

    def test_backtest_does_not_trigger_floor_stop_when_floor_is_above_entry(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {
                        "signal_date": "2020-01-31",
                        "effective_date": "2020-02-03",
                        "ticker": "AAA",
                        "vcrf_state": "ATTACK",
                        "floor_price": 12.0,
                        "recognition_price": 20.0,
                        "total_score": 95.0,
                        "tradable_flag": 1,
                    }
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04"]),
        )
        daily_bars = pd.DataFrame(
            [
                {"date": "2020-02-03", "ticker": "AAA", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.1},
                {"date": "2020-02-04", "ticker": "AAA", "open": 10.2, "high": 10.3, "low": 9.9, "close": 10.0},
            ]
        )

        result = run_vcrf_backtest(
            month_end,
            daily_bars,
            protocol={
                "initial_cash": 1_000_000,
                "round_size": 1,
                "total_rounds": 1,
                "exclude_used_tickers_across_rounds": True,
                "lot_size": 100,
                "max_holding_bars": 504,
                "same_bar_conflict": "stop_first",
                "costs": {
                    "broker_commission_bps": 0.0,
                    "broker_min_commission": 0.0,
                    "transfer_fee_bps": 0.0,
                    "slippage_bps_buy": 0.0,
                    "slippage_bps_sell": 0.0,
                    "stamp_duty": [],
                },
            },
        )

        trades = result["rounds"][0]["trades"].set_index("ticker")
        self.assertEqual(trades.loc["AAA", "exit_reason"], "end_of_data")

    def test_backtest_uses_max_loss_stop_when_floor_is_above_entry(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {
                        "signal_date": "2020-01-31",
                        "effective_date": "2020-02-03",
                        "ticker": "AAA",
                        "vcrf_state": "ATTACK",
                        "floor_price": 12.0,
                        "recognition_price": 20.0,
                        "total_score": 95.0,
                        "tradable_flag": 1,
                    }
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04"]),
        )
        daily_bars = pd.DataFrame(
            [
                {"date": "2020-02-03", "ticker": "AAA", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.1},
                {"date": "2020-02-04", "ticker": "AAA", "open": 10.2, "high": 10.3, "low": 7.8, "close": 8.1},
            ]
        )

        result = run_vcrf_backtest(
            month_end,
            daily_bars,
            protocol={
                "initial_cash": 1_000_000,
                "round_size": 1,
                "total_rounds": 1,
                "exclude_used_tickers_across_rounds": True,
                "lot_size": 100,
                "max_holding_bars": 504,
                "max_loss_pct": 0.20,
                "same_bar_conflict": "stop_first",
                "costs": {
                    "broker_commission_bps": 0.0,
                    "broker_min_commission": 0.0,
                    "transfer_fee_bps": 0.0,
                    "slippage_bps_buy": 0.0,
                    "slippage_bps_sell": 0.0,
                    "stamp_duty": [],
                },
            },
        )

        trades = result["rounds"][0]["trades"].set_index("ticker")
        self.assertEqual(trades.loc["AAA", "exit_reason"], "max_loss_stop")
        self.assertAlmostEqual(float(trades.loc["AAA", "exit_price"]), 8.0, places=4)

    def test_backtest_uses_last_known_price_for_suspended_position_equity(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 1.0, "recognition_price": 100.0, "total_score": 95.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "ATTACK", "floor_price": 1.0, "recognition_price": 100.0, "total_score": 90.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04", "2020-02-05"]),
        )
        daily_bars = pd.DataFrame(
            [
                {"date": "2020-02-03", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0},
                {"date": "2020-02-03", "ticker": "BBB", "open": 20.0, "high": 21.0, "low": 20.0, "close": 21.0},
                {"date": "2020-02-04", "ticker": "BBB", "open": 21.0, "high": 22.0, "low": 21.0, "close": 22.0},
                {"date": "2020-02-05", "ticker": "AAA", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0},
                {"date": "2020-02-05", "ticker": "BBB", "open": 22.0, "high": 22.0, "low": 22.0, "close": 22.0},
            ]
        )

        result = run_vcrf_backtest(
            month_end,
            daily_bars,
            protocol={
                "initial_cash": 1_000_000,
                "round_size": 2,
                "total_rounds": 1,
                "exclude_used_tickers_across_rounds": True,
                "lot_size": 100,
                "max_holding_bars": 504,
                "same_bar_conflict": "stop_first",
                "costs": {
                    "broker_commission_bps": 0.0,
                    "broker_min_commission": 0.0,
                    "transfer_fee_bps": 0.0,
                    "slippage_bps_buy": 0.0,
                    "slippage_bps_sell": 0.0,
                    "stamp_duty": [],
                },
            },
        )

        equity = result["rounds"][0]["equity"].set_index("date")
        self.assertEqual(float(equity.loc[pd.Timestamp("2020-02-04"), "equity"]), 1_100_000.0)

    def test_backtest_closes_suspended_position_at_end_of_data_with_last_known_price(self) -> None:
        month_end = normalize_signal_month_end(
            pd.DataFrame(
                [
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "AAA", "vcrf_state": "ATTACK", "floor_price": 1.0, "recognition_price": 100.0, "total_score": 95.0, "tradable_flag": 1},
                    {"signal_date": "2020-01-31", "effective_date": "2020-02-03", "ticker": "BBB", "vcrf_state": "ATTACK", "floor_price": 1.0, "recognition_price": 100.0, "total_score": 90.0, "tradable_flag": 1},
                ]
            ),
            pd.DatetimeIndex(["2020-02-03", "2020-02-04"]),
        )
        daily_bars = pd.DataFrame(
            [
                {"date": "2020-02-03", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 10.0, "close": 11.0},
                {"date": "2020-02-03", "ticker": "BBB", "open": 20.0, "high": 21.0, "low": 20.0, "close": 21.0},
                {"date": "2020-02-04", "ticker": "BBB", "open": 21.0, "high": 22.0, "low": 21.0, "close": 22.0},
            ]
        )

        result = run_vcrf_backtest(
            month_end,
            daily_bars,
            protocol={
                "initial_cash": 1_000_000,
                "round_size": 2,
                "total_rounds": 1,
                "exclude_used_tickers_across_rounds": True,
                "lot_size": 100,
                "max_holding_bars": 504,
                "same_bar_conflict": "stop_first",
                "costs": {
                    "broker_commission_bps": 0.0,
                    "broker_min_commission": 0.0,
                    "transfer_fee_bps": 0.0,
                    "slippage_bps_buy": 0.0,
                    "slippage_bps_sell": 0.0,
                    "stamp_duty": [],
                },
            },
        )

        trades = result["rounds"][0]["trades"].set_index("ticker")
        self.assertEqual(trades.loc["AAA", "exit_reason"], "end_of_data_suspended")
        self.assertEqual(float(trades.loc["AAA", "exit_price"]), 11.0)
