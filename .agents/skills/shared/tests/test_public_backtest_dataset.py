import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

from engines.public_backtest_dataset_engine import (
    _build_kline_snapshot,
    build_public_backtest_inputs,
    discover_baostock_universe_tickers,
    discover_local_watchlist_tickers,
    discover_public_financial_usable_tickers,
    fetch_public_history_bundle,
    filter_records_as_of,
    month_end_trade_dates,
)


class PublicBacktestHelpersTests(unittest.TestCase):
    def test_build_kline_snapshot_uses_trailing_five_year_window(self) -> None:
        dates = pd.to_datetime(
            [
                "2018-01-31",
                "2020-01-31",
                "2021-01-31",
                "2022-01-31",
                "2023-01-31",
                "2024-01-31",
            ]
        )
        bars = pd.DataFrame(
            {
                "date": dates,
                "ticker": ["600001"] * len(dates),
                "open": [100.0, 9.0, 10.0, 11.0, 12.0, 13.0],
                "high": [100.0, 9.0, 10.0, 11.0, 12.0, 13.0],
                "low": [100.0, 9.0, 10.0, 11.0, 12.0, 13.0],
                "close": [100.0, 9.0, 10.0, 11.0, 12.0, 13.0],
                "volume": [1_000_000] * len(dates),
                "amount": [20_000_000] * len(dates),
            }
        )

        snapshot = _build_kline_snapshot(bars)

        self.assertEqual(snapshot["high_5y"], 13.0)
        self.assertEqual(snapshot["low_5y"], 9.0)
        self.assertAlmostEqual(snapshot["current_vs_5yr_high"], 100.0, places=2)

    def test_filter_records_as_of_uses_statutory_lag_when_announcement_date_missing(self) -> None:
        records = [
            {"报告日": "20240331", "归属于母公司所有者的净利润": 100},
            {"报告日": "20230930", "归属于母公司所有者的净利润": 80},
        ]

        before_q1_window = filter_records_as_of(records, "2024-04-28")
        after_q1_window = filter_records_as_of(records, "2024-04-30")

        self.assertEqual(len(before_q1_window), 1)
        self.assertEqual(before_q1_window[0]["报告日"], "20230930")
        self.assertEqual(len(after_q1_window), 2)

    def test_month_end_trade_dates_uses_last_available_bar_each_month(self) -> None:
        daily_bars = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2020-01-30", "2020-01-31", "2020-02-27", "2020-02-28", "2020-03-31"]
                ),
                "ticker": ["600001"] * 5,
                "open": [1, 1, 1, 1, 1],
                "high": [1, 1, 1, 1, 1],
                "low": [1, 1, 1, 1, 1],
                "close": [1, 1, 1, 1, 1],
            }
        )

        result = month_end_trade_dates(daily_bars)

        self.assertEqual(
            [str(item.date()) for item in result],
            ["2020-01-31", "2020-02-28", "2020-03-31"],
        )

    def test_discover_local_watchlist_tickers_collects_repo_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for relative in ("evidence/600328", "data/raw/000425", "data/processed/601065", "evidence/not_a_code"):
                (root / relative).mkdir(parents=True, exist_ok=True)

            tickers = discover_local_watchlist_tickers(root)

            self.assertEqual(tickers, ["000425", "600328", "601065"])

    def test_discover_baostock_universe_tickers_respects_trade_status_and_limit(self) -> None:
        fake_rows = [
            {"code": "600001", "trade_status": "1"},
            {"code": "600002", "trade_status": "0"},
            {"code": "600003", "trade_status": "1"},
        ]
        with patch("engines.public_backtest_dataset_engine.get_all_a_share_stocks", return_value={"data": fake_rows}):
            tickers = discover_baostock_universe_tickers(limit=1)

        self.assertEqual(tickers, ["600001"])

    def test_discover_public_financial_usable_tickers_requires_income_and_balance(self) -> None:
        def _result(data):
            return {"data": data}

        with patch(
            "engines.public_backtest_dataset_engine.get_income_statement",
            side_effect=[_result([{"报告日": "20231231"}]), _result([]), _result([{"报告日": "20231231"}])],
        ), patch(
            "engines.public_backtest_dataset_engine.get_balance_sheet",
            side_effect=[_result([{"报告日": "20231231"}]), _result([{"报告日": "20231231"}]), _result([])],
        ), patch(
            "engines.public_backtest_dataset_engine.get_cashflow_statement",
            side_effect=[_result([{"报告日": "20231231"}]), _result([{"报告日": "20231231"}]), _result([{"报告日": "20231231"}])],
        ):
            tickers = discover_public_financial_usable_tickers(["600001", "600002", "600003"], target_count=2)

        self.assertEqual(tickers, ["600001"])


class PublicBacktestBuilderTests(unittest.TestCase):
    def test_fetch_public_history_bundle_falls_back_to_local_cache_when_live_statements_are_empty(self) -> None:
        cached_scan = {
            "company_profile": {"data": {"股票简称": "中盐化工"}},
            "revenue_breakdown": {"data": [{"报告期": "20231231", "产品名称": "纯碱"}]},
            "income_statement": {"data": [{"报告日": "20231231", "归属于母公司所有者的净利润": 1}]},
            "balance_sheet": {"data": [{"报告日": "20231231", "归属于母公司所有者权益合计": 1}]},
            "cashflow_statement": {"data": [{"报告日": "20231231", "经营活动产生的现金流量净额": 1}]},
        }
        with patch(
            "engines.public_backtest_dataset_engine.get_daily_history",
            return_value={"data": [{"date": "2024-01-02", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "amount": 1}]},
        ), patch(
            "engines.public_backtest_dataset_engine.get_company_profile",
            return_value={"data": {}},
        ), patch(
            "engines.public_backtest_dataset_engine.get_revenue_breakdown",
            return_value={"data": []},
        ), patch(
            "engines.public_backtest_dataset_engine.get_income_statement",
            return_value={"data": []},
        ), patch(
            "engines.public_backtest_dataset_engine.get_balance_sheet",
            return_value={"data": []},
        ), patch(
            "engines.public_backtest_dataset_engine.get_cashflow_statement",
            return_value={"data": []},
        ), patch(
            "engines.public_backtest_dataset_engine._load_local_cached_scan",
            return_value=cached_scan,
        ):
            bundle = fetch_public_history_bundle("600328", "2024-01-01", "2025-12-31")

        self.assertEqual(bundle["company_profile"]["data"]["股票简称"], "中盐化工")
        self.assertEqual(len(bundle["income_statement"]["data"]), 1)
        self.assertEqual(bundle["daily_bars"].shape[0], 1)

    def test_fetch_public_history_bundle_can_prefer_local_cache_without_live_calls(self) -> None:
        cached_scan = {
            "company_profile": {"data": {"股票简称": "中盐化工"}},
            "revenue_breakdown": {"data": [{"报告期": "20231231", "产品名称": "纯碱"}]},
            "income_statement": {"data": [{"报告日": "20231231", "归属于母公司所有者的净利润": 1}]},
            "balance_sheet": {"data": [{"报告日": "20231231", "归属于母公司所有者权益合计": 1}]},
            "cashflow_statement": {"data": [{"报告日": "20231231", "经营活动产生的现金流量净额": 1}]},
        }
        with patch(
            "engines.public_backtest_dataset_engine.get_daily_history",
            return_value={"data": [{"date": "2024-01-02", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "amount": 1}]},
        ), patch(
            "engines.public_backtest_dataset_engine.get_company_profile",
            side_effect=AssertionError("live company profile should not be called"),
        ), patch(
            "engines.public_backtest_dataset_engine.get_revenue_breakdown",
            side_effect=AssertionError("live revenue breakdown should not be called"),
        ), patch(
            "engines.public_backtest_dataset_engine.get_income_statement",
            side_effect=AssertionError("live income statement should not be called"),
        ), patch(
            "engines.public_backtest_dataset_engine.get_balance_sheet",
            side_effect=AssertionError("live balance sheet should not be called"),
        ), patch(
            "engines.public_backtest_dataset_engine.get_cashflow_statement",
            side_effect=AssertionError("live cashflow should not be called"),
        ), patch(
            "engines.public_backtest_dataset_engine._load_local_cached_scan",
            return_value=cached_scan,
        ):
            bundle = fetch_public_history_bundle("600328", "2024-01-01", "2025-12-31", prefer_local_cache=True)

        self.assertEqual(bundle["company_profile"]["data"]["股票简称"], "中盐化工")
        self.assertEqual(bundle["daily_bars"].shape[0], 1)

    def test_build_public_backtest_inputs_outputs_backtest_ready_tables(self) -> None:
        daily_bars = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2020-01-30",
                        "2020-01-31",
                        "2020-02-03",
                        "2020-02-28",
                        "2020-03-02",
                        "2020-03-31",
                    ]
                ),
                "ticker": ["600328"] * 6,
                "open": [10.0, 10.2, 10.3, 10.5, 10.6, 10.8],
                "high": [10.2, 10.4, 10.5, 10.7, 10.8, 11.0],
                "low": [9.9, 10.0, 10.1, 10.3, 10.4, 10.6],
                "close": [10.1, 10.3, 10.4, 10.6, 10.7, 10.9],
                "volume": [1_000_000, 1_100_000, 1_050_000, 1_200_000, 1_250_000, 1_300_000],
                "amount": [25_000_000, 26_000_000, 27_000_000, 28_000_000, 29_000_000, 30_000_000],
            }
        )
        bundle = {
            "ticker": "600328",
            "company_profile": {"data": {"股票简称": "中盐化工", "行业": "化工", "主营业务": "纯碱与氯碱"}},
            "revenue_breakdown": {"data": [{"报告期": "20231231", "产品名称": "纯碱", "主营收入": 80, "毛利率": 25.0}]},
            "income_statement": {
                "data": [
                    {"报告日": "20190930", "归属于母公司所有者的净利润": 800_000_000, "营业总收入": 10_000_000_000},
                    {"报告日": "20181231", "归属于母公司所有者的净利润": 700_000_000, "营业总收入": 9_000_000_000},
                    {"报告日": "20171231", "归属于母公司所有者的净利润": 600_000_000, "营业总收入": 8_000_000_000},
                ]
            },
            "balance_sheet": {"data": [{"报告日": "20190930", "归属于母公司所有者权益合计": 6_000_000_000, "实收资本(或股本)": 1_000_000_000}]},
            "cashflow_statement": {"data": [{"报告日": "20190930", "经营活动产生的现金流量净额": 900_000_000}]},
            "daily_bars": daily_bars,
        }

        def fake_provider(_ticker: str, _start_date: str, _end_date: str) -> dict:
            return bundle

        fake_gate = {
            "position_state": "attack",
            "underwrite_axis": {"score": 82.0},
            "realization_axis": {"score": 74.0},
            "driver_stack": {"primary_type": "cyclical", "sector_route": "core_resource"},
            "scorecard": {"verdict": "high conviction / strong candidate"},
            "hard_vetos": [],
        }
        fake_valuation = {
            "floor_case": {"implied_price": 8.5},
            "recognition_case": {"implied_price": 14.0},
            "summary": {"floor_protection": 0.80, "recognition_upside": 0.30},
        }

        with patch("engines.public_backtest_dataset_engine.evaluate_universal_gates", return_value=fake_gate), patch(
            "engines.public_backtest_dataset_engine.build_three_case_valuation",
            return_value=fake_valuation,
        ):
            result = build_public_backtest_inputs(
                tickers=["600328"],
                start_date="2020-01-01",
                end_date="2020-03-31",
                bundle_provider=fake_provider,
            )

        self.assertIn("signals_month_end", result)
        self.assertIn("daily_bars", result)
        signals = result["signals_month_end"]
        self.assertEqual(signals["ticker"].tolist(), ["600328", "600328", "600328"])
        self.assertEqual(signals["vcrf_state"].tolist(), ["ATTACK", "ATTACK", "ATTACK"])
        self.assertTrue((signals["tradable_flag"] == 1).all())
        self.assertTrue({"signal_date", "ticker", "floor_price", "recognition_price", "total_score"}.issubset(signals.columns))

    def test_build_public_backtest_inputs_keeps_tickers_with_missing_cashflow_statement(self) -> None:
        daily_bars = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2024-04-29",
                        "2024-04-30",
                        "2024-05-06",
                        "2024-05-31",
                    ]
                ),
                "ticker": ["600328"] * 4,
                "open": [10.0, 10.2, 10.3, 10.5],
                "high": [10.2, 10.4, 10.5, 10.7],
                "low": [9.9, 10.0, 10.1, 10.3],
                "close": [10.1, 10.3, 10.4, 10.6],
                "volume": [1_000_000, 1_100_000, 1_050_000, 1_200_000],
                "amount": [25_000_000, 26_000_000, 27_000_000, 28_000_000],
            }
        )
        bundle = {
            "ticker": "600328",
            "company_profile": {"data": {"股票简称": "中盐化工", "行业": "化工", "主营业务": "纯碱与氯碱"}},
            "revenue_breakdown": {"data": [{"报告期": "20231231", "产品名称": "纯碱", "主营收入": 80, "毛利率": 25.0}]},
            "income_statement": {
                "data": [
                    {"报告日": "20231231", "归属于母公司所有者的净利润": 800_000_000, "营业总收入": 10_000_000_000},
                ]
            },
            "balance_sheet": {
                "data": [
                    {"报告日": "20231231", "归属于母公司所有者权益合计": 6_000_000_000, "实收资本(或股本)": 1_000_000_000},
                ]
            },
            "cashflow_statement": {"data": []},
            "daily_bars": daily_bars,
        }

        def fake_provider(_ticker: str, _start_date: str, _end_date: str) -> dict:
            return bundle

        fake_gate = {
            "position_state": "reject",
            "underwrite_axis": {"score": 56.0},
            "realization_axis": {"score": 47.0},
            "driver_stack": {"primary_type": "cyclical", "sector_route": "core_resource"},
            "scorecard": {"verdict": "watchlist / incomplete edge"},
            "hard_vetos": [],
        }
        fake_valuation = {
            "floor_case": {"implied_price": 8.5},
            "recognition_case": {"implied_price": 14.0},
            "summary": {"floor_protection": 0.80, "recognition_upside": 0.30},
        }

        with patch("engines.public_backtest_dataset_engine.evaluate_universal_gates", return_value=fake_gate) as gate_patch, patch(
            "engines.public_backtest_dataset_engine.build_three_case_valuation",
            return_value=fake_valuation,
        ):
            result = build_public_backtest_inputs(
                tickers=["600328"],
                start_date="2024-04-01",
                end_date="2024-05-31",
                bundle_provider=fake_provider,
            )

        self.assertEqual(gate_patch.call_count, 2)
        self.assertEqual(result["signals_month_end"]["ticker"].tolist(), ["600328", "600328"])


if __name__ == "__main__":
    unittest.main()
