import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

from adapters.tushare_adapter import (
    _force_https_transport,
    discover_tushare_universe_tickers,
    resolve_tushare_token,
    resolve_tushare_tokens,
)
from engines.tushare_backtest_dataset_engine import (
    _to_ts_code,
    _normalize_tushare_balance_records,
    _normalize_tushare_daily_bars,
    build_tushare_backtest_inputs,
)


class TushareAdapterTests(unittest.TestCase):
    def test_force_https_transport_updates_sdk_client_endpoint(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self._DataApi__http_url = "http://api.waditu.com/dataapi"

        client = FakeClient()

        updated = _force_https_transport(client)

        self.assertIs(updated, client)
        self.assertEqual(client._DataApi__http_url, "https://api.waditu.com/dataapi")

    def test_resolve_tushare_tokens_reads_multi_token_repo_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / ".env").write_text("TUSHARE_TOKENS=token-a, token-b ,token-c\n", encoding="utf-8")

            tokens = resolve_tushare_tokens(root)

        self.assertEqual(tokens, ("token-a", "token-b", "token-c"))

    def test_resolve_tushare_token_reads_repo_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / ".env").write_text("TUSHARE_TOKEN=test-token\n", encoding="utf-8")

            token = resolve_tushare_token(root)

        self.assertEqual(token, "test-token")

    def test_query_failover_uses_next_token_when_first_token_errors(self) -> None:
        calls: list[str] = []

        class FakeClient:
            def __init__(self, token: str) -> None:
                self.token = token

            def stock_basic(self, **_kwargs):
                calls.append(self.token)
                if self.token == "bad-token":
                    raise RuntimeError("permission denied")
                return pd.DataFrame([{"ts_code": "000001.SZ", "symbol": "000001"}])

        with patch("adapters.tushare_adapter.resolve_tushare_tokens", return_value=("bad-token", "good-token")), patch(
            "adapters.tushare_adapter._pro_client",
            side_effect=lambda token: FakeClient(token),
        ):
            result = discover_tushare_universe_tickers(list_statuses=("L",))

        self.assertEqual(calls, ["bad-token", "good-token"])
        self.assertEqual(result, ["000001"])

    def test_discover_tushare_universe_tickers_merges_requested_statuses(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_stock_basic",
            side_effect=[
                {"data": [{"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "list_status": "L"}]},
                {"data": [{"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A", "list_status": "D"}]},
            ],
        ):
            tickers = discover_tushare_universe_tickers(list_statuses=("L", "D"))

        self.assertEqual(tickers, ["000001", "000002"])


class TushareBacktestBuilderTests(unittest.TestCase):
    def test_to_ts_code_maps_beijing_exchange_prefixes(self) -> None:
        self.assertEqual(_to_ts_code("830799"), "830799.BJ")
        self.assertEqual(_to_ts_code("430047"), "430047.BJ")
        self.assertEqual(_to_ts_code("600328"), "600328.SH")
        self.assertEqual(_to_ts_code("000001"), "000001.SZ")

    def test_normalize_tushare_daily_bars_applies_adjustment_factor_in_one_coordinate_system(self) -> None:
        daily_records = [
            {"trade_date": "20240102", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 100, "amount": 200},
            {"trade_date": "20240103", "open": 5.0, "high": 5.5, "low": 4.8, "close": 5.2, "vol": 120, "amount": 220},
        ]
        daily_basic_records = [
            {"trade_date": "20240102", "total_mv": 100, "circ_mv": 80, "pb": 1.1, "turnover_rate": 2.0},
            {"trade_date": "20240103", "total_mv": 52, "circ_mv": 40, "pb": 1.0, "turnover_rate": 2.1},
        ]
        adj_factor_records = [
            {"trade_date": "20240102", "adj_factor": 2.0},
            {"trade_date": "20240103", "adj_factor": 1.0},
        ]

        bars, basic = _normalize_tushare_daily_bars(daily_records, daily_basic_records, adj_factor_records, "600328")

        self.assertEqual(bars["ticker"].tolist(), ["600328", "600328"])
        self.assertAlmostEqual(bars.loc[0, "open"], 20.0, places=4)
        self.assertAlmostEqual(bars.loc[0, "close"], 21.0, places=4)
        self.assertAlmostEqual(bars.loc[1, "close"], 5.2, places=4)
        self.assertAlmostEqual(basic.loc[1, "total_mv"], 520_000.0, places=4)

    def test_normalize_tushare_balance_records_keeps_total_share_in_native_share_unit(self) -> None:
        records = [{"end_date": "20241231", "ann_date": "20250331", "total_share": 1_250_000_000}]

        normalized = _normalize_tushare_balance_records(records)

        self.assertEqual(normalized[0]["实收资本(或股本)"], 1_250_000_000)

    def test_build_tushare_backtest_inputs_outputs_backtest_ready_tables(self) -> None:
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
                    {"报告日": "20190930", "公告日期": "20191030", "归属于母公司所有者的净利润": 800_000_000, "营业总收入": 10_000_000_000},
                    {"报告日": "20181231", "公告日期": "20190425", "归属于母公司所有者的净利润": 700_000_000, "营业总收入": 9_000_000_000},
                ]
            },
            "balance_sheet": {
                "data": [
                    {"报告日": "20190930", "公告日期": "20191030", "归属于母公司所有者权益合计": 6_000_000_000, "实收资本(或股本)": 1_000_000_000},
                ]
            },
            "cashflow_statement": {"data": [{"报告日": "20190930", "公告日期": "20191030", "经营活动产生的现金流量净额": 900_000_000}]},
            "fina_indicator": {"data": [{"报告期": "20190930", "销售净利率(%)": 8.0}]},
            "daily_bars": daily_bars,
        }

        def fake_provider(_ticker: str, _start_date: str, _end_date: str) -> dict:
            return bundle

        fake_gate = {
            "position_state": "attack",
            "underwrite_axis": {
                "score": 82.0,
                "components": {
                    "intrinsic_value_floor": {"score": 80.0},
                    "survival_boundary": {"score": 81.0},
                    "governance_anti_fraud": {"score": 82.0},
                    "business_or_asset_quality": {"score": 83.0},
                    "normalized_earnings_power": {"score": 84.0},
                },
            },
            "realization_axis": {
                "score": 74.0,
                "flow_stage": "trend",
                "components": {
                    "repair_state": {"score": 70.0},
                    "regime_cycle_position": {"score": 71.0},
                    "marginal_buyer_probability": {"score": 72.0},
                    "flow_confirmation": {"score": 73.0},
                    "elasticity": {"score": 74.0},
                    "catalyst_quality": {"score": 75.0},
                },
            },
            "driver_stack": {"primary_type": "cyclical", "sector_route": "core_resource"},
            "scorecard": {"verdict": "high conviction / strong candidate"},
            "hard_vetos": [],
        }
        fake_valuation = {
            "floor_case": {"implied_price": 8.5},
            "recognition_case": {"implied_price": 14.0},
            "summary": {"floor_protection": 0.80, "recognition_upside": 0.30},
        }

        with patch("engines.tushare_backtest_dataset_engine.evaluate_universal_gates", return_value=fake_gate), patch(
            "engines.tushare_backtest_dataset_engine.build_three_case_valuation",
            return_value=fake_valuation,
        ):
            result = build_tushare_backtest_inputs(
                tickers=["600328"],
                start_date="2020-01-01",
                end_date="2020-03-31",
                bundle_provider=fake_provider,
            )

        signals = result["signals_month_end"]
        self.assertEqual(signals["ticker"].tolist(), ["600328", "600328", "600328"])
        self.assertEqual(signals["vcrf_state"].tolist(), ["ATTACK", "ATTACK", "ATTACK"])
        self.assertTrue((signals["tradable_flag"] == 1).all())
        self.assertTrue({"underwrite_score", "realization_score", "position_state"}.issubset(signals.columns))
        self.assertTrue(
            {
                "underwrite_intrinsic_value_floor_score",
                "underwrite_survival_boundary_score",
                "realization_flow_confirmation_score",
                "realization_catalyst_quality_score",
                "flow_stage",
            }.issubset(signals.columns)
        )
        self.assertEqual(float(signals.iloc[0]["underwrite_intrinsic_value_floor_score"]), 80.0)
        self.assertEqual(float(signals.iloc[0]["realization_flow_confirmation_score"]), 73.0)
        self.assertEqual(signals.iloc[0]["flow_stage"], "trend")
        diagnostics = result["manifest"]["diagnostics"]
        self.assertEqual(diagnostics["vcrf_state_counts"]["ATTACK"], 3)
        self.assertEqual(diagnostics["attack_tradable_rows"], 3)


if __name__ == "__main__":
    unittest.main()
