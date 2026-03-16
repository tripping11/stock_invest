import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from datetime import date, timedelta
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pandas as pd


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))
RADAR_ENGINE_PATH = SHARED_DIR.parent / "market-opportunity-scanner" / "scripts" / "engines" / "radar_scan_engine.py"

from engines.report_engine import generate_deep_dive_report
from engines.synthesis_engine import build_investment_synthesis
from engines.valuation_engine import build_three_case_valuation
from adapters import akshare_adapter
from adapters import baostock_adapter
from adapters import tier0_report_pack_adapter
from utils import research_utils
from utils.framework_utils import determine_opportunity_type
from utils.hard_rule_utils import evaluate_business_simplicity
from utils.signal_health_utils import evaluate_signal_health_v2
from validators import tier0_verifier
from validators import universal_gate as universal_gate_module
from validators.universal_gate import evaluate_universal_gates


def _load_radar_scan_engine():
    spec = importlib.util.spec_from_file_location("radar_scan_engine_under_test", RADAR_ENGINE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class _FakeResultSet:
    def __init__(self, fields, rows, error_code: str = "0", error_msg: str = "success") -> None:
        self.fields = fields
        self._rows = rows
        self._index = -1
        self.error_code = error_code
        self.error_msg = error_msg

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class OpportunityTypeTests(unittest.TestCase):
    def test_detects_cyclical_type(self) -> None:
        result = determine_opportunity_type(
            "600348",
            {"行业": "煤炭", "主营业务": "煤炭开采与销售"},
            revenue_records=[{"报告期": "20241231", "主营构成": "煤炭", "主营收入": 90}],
        )
        self.assertEqual(result["primary_type"], "cyclical")
        self.assertEqual(result["confidence"], "high")

    def test_detects_special_situation(self) -> None:
        result = determine_opportunity_type(
            "600000",
            {"行业": "综合", "主营业务": "重大资产重组与资产注入推进中"},
            revenue_records=[],
        )
        self.assertEqual(result["primary_type"], "special_situation")

    def test_detects_compounder_type(self) -> None:
        result = determine_opportunity_type(
            "300001",
            {"行业": "软件开发", "主营业务": "SaaS 订阅软件服务，会员复购率高"},
            revenue_records=[{"报告期": "20241231", "主营构成": "SaaS", "主营收入": 92}],
        )
        self.assertEqual(result["primary_type"], "compounder")

    def test_detects_turnaround_type(self) -> None:
        result = determine_opportunity_type(
            "600002",
            {"行业": "综合", "主营业务": "*ST 公司推进债务重组与经营改善"},
            revenue_records=[],
        )
        self.assertEqual(result["primary_type"], "turnaround")

    def test_detects_asset_play_type(self) -> None:
        result = determine_opportunity_type(
            "600003",
            {"行业": "银行", "主营业务": "NAV 折价与 REIT 资产盘活"},
            revenue_records=[],
        )
        self.assertEqual(result["primary_type"], "asset_play")


class BaoStockFallbackTests(unittest.TestCase):
    def test_baostock_equity_filter_includes_star_market_codes(self) -> None:
        self.assertTrue(baostock_adapter._is_a_share_equity("sh.688001"))
        self.assertTrue(baostock_adapter._is_a_share_equity("sh.689009"))
        self.assertTrue(baostock_adapter._is_a_share_equity("sz.301001"))
        self.assertFalse(baostock_adapter._is_a_share_equity("sh.000001"))

    def test_radar_universe_falls_back_to_baostock_when_akshare_snapshot_fails(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        fallback_rows = {
            "data": [
                {"code": "600328", "name": "中盐化工"},
                {"code": "000731", "name": "四川美丰"},
                {"code": "600581", "name": "八一钢铁"},
                {"code": "600000", "name": "ST样本"},
            ],
            "status": "ok_fallback_baostock_universe",
        }

        with patch.object(radar_scan_engine.ak, "stock_zh_a_spot_em", side_effect=RuntimeError("snapshot down")):
            with patch.object(radar_scan_engine, "get_all_a_share_stocks", return_value=fallback_rows, create=True):
                universe = radar_scan_engine._load_universe("A-share", 3)

        self.assertEqual(
            universe,
            [
                {"code": "600328", "name": "中盐化工"},
                {"code": "000731", "name": "四川美丰"},
                {"code": "600581", "name": "八一钢铁"},
            ],
        )

    def test_realtime_quote_uses_baostock_daily_snapshot_when_primary_sources_fail(self) -> None:
        daily_history = {
            "data": [
                {"date": "2026-03-12", "code": "sh.600328", "close": "10.41", "pbMRQ": "1.270840", "peTTM": "5779.089659"},
                {"date": "2026-03-13", "code": "sh.600328", "close": "10.14", "pbMRQ": "1.237878", "peTTM": "5629.199725"},
            ],
            "status": "ok_fallback_baostock_history",
        }
        basic_info = {
            "data": {"code": "sh.600328", "code_name": "中盐化工"},
            "status": "ok_fallback_baostock_stock_basic",
        }

        with patch.object(akshare_adapter.ak, "stock_zh_a_spot_em", side_effect=RuntimeError("snapshot down")):
            with patch.object(akshare_adapter, "_load_efinance", return_value=None):
                with patch.object(akshare_adapter, "get_company_profile", return_value={"data": {}, "status": "error: no profile"}):
                    with patch.object(akshare_adapter, "_derive_quote_snapshot", return_value={}):
                        with patch.object(akshare_adapter, "get_daily_history", return_value=daily_history, create=True):
                            with patch.object(akshare_adapter, "get_stock_basic", return_value=basic_info, create=True):
                                quote = akshare_adapter.get_realtime_quote("600328")

        self.assertEqual(quote["status"], "ok_fallback_baostock_daily_snapshot")
        self.assertEqual(quote["data"]["代码"], "600328")
        self.assertEqual(quote["data"]["名称"], "中盐化工")
        self.assertEqual(quote["data"]["最新价"], 10.14)
        self.assertEqual(quote["data"]["最新交易日"], "2026-03-13")

    def test_valuation_history_uses_baostock_daily_pb_series(self) -> None:
        daily_history = {
            "data": [
                {"date": "2026-03-11", "code": "sh.600328", "close": "9.94", "pbMRQ": "1.213463", "peTTM": "5518.170145"},
                {"date": "2026-03-12", "code": "sh.600328", "close": "10.41", "pbMRQ": "1.270840", "peTTM": "5779.089659"},
                {"date": "2026-03-13", "code": "sh.600328", "close": "10.14", "pbMRQ": "1.237878", "peTTM": "5629.199725"},
            ],
            "status": "ok_fallback_baostock_history",
        }

        with patch.object(akshare_adapter.ak, "stock_zh_a_hist", side_effect=RuntimeError("kline down")):
            with patch.object(akshare_adapter.ak, "stock_financial_analysis_indicator", side_effect=RuntimeError("fin down")):
                with patch.object(akshare_adapter, "get_daily_history", return_value=daily_history, create=True):
                    valuation = akshare_adapter.get_valuation_history("600328")

        self.assertEqual(valuation["status"], "ok_fallback_baostock_history")
        self.assertAlmostEqual(valuation["data"]["pb"], 1.2379, places=4)
        self.assertAlmostEqual(valuation["data"]["pb_percentile"], 33.33, places=2)
        self.assertAlmostEqual(valuation["data"]["pb_min"], 1.2135, places=4)
        self.assertAlmostEqual(valuation["data"]["pb_max"], 1.2708, places=4)
        self.assertAlmostEqual(valuation["data"]["pb_median"], 1.2379, places=4)
        self.assertAlmostEqual(valuation["data"]["latest_close"], 10.14, places=2)

    def test_stock_kline_uses_baostock_history_when_akshare_history_fails(self) -> None:
        start_day = date(2025, 10, 1)
        daily_rows = []
        for offset in range(130):
            current_day = start_day + timedelta(days=offset)
            close = 9.5 + (offset % 10) * 0.1
            volume = 1000 + offset * 10
            amount = volume * close
            daily_rows.append(
                {
                    "date": current_day.isoformat(),
                    "code": "sh.600328",
                    "open": f"{close - 0.05:.2f}",
                    "high": f"{close + 0.05:.2f}",
                    "low": f"{close - 0.10:.2f}",
                    "close": f"{close:.2f}",
                    "volume": str(volume),
                    "amount": f"{amount:.2f}",
                    "turn": "1.0",
                    "pctChg": "0.5",
                }
            )
        history = {"data": daily_rows, "status": "ok_fallback_baostock_history"}

        with patch.object(akshare_adapter.ak, "stock_zh_a_hist", side_effect=RuntimeError("hist down")):
            with patch.object(akshare_adapter, "get_daily_history", return_value=history, create=True):
                kline = akshare_adapter.get_stock_kline("600328")

        self.assertEqual(kline["status"], "ok_fallback_baostock_history")
        self.assertAlmostEqual(kline["data"]["latest_close"], 10.4, places=2)
        self.assertAlmostEqual(kline["data"]["high_5y"], 10.4, places=2)
        self.assertAlmostEqual(kline["data"]["low_5y"], 9.5, places=2)
        self.assertEqual(kline["data"]["total_bars"], 130)
        self.assertGreater(kline["data"]["volume_ratio_20_vs_120"], 1.0)

    def test_baostock_universe_retries_previous_trade_day_when_today_is_empty(self) -> None:
        class FakeBaoStock:
            def __init__(self) -> None:
                self.days_requested: list[str] = []

            def query_all_stock(self, day: str):
                self.days_requested.append(day)
                if day == "2026-03-16":
                    return _FakeResultSet(["code", "tradeStatus", "code_name"], [])
                return _FakeResultSet(
                    ["code", "tradeStatus", "code_name"],
                    [
                        ["sh.000001", "1", "上证综合指数"],
                        ["sh.600000", "1", "浦发银行"],
                        ["sz.000001", "1", "平安银行"],
                    ],
                )

            def query_trade_dates(self, start_date: str, end_date: str):
                return _FakeResultSet(
                    ["calendar_date", "is_trading_day"],
                    [
                        ["2026-03-13", "1"],
                        ["2026-03-16", "1"],
                    ],
                )

        fake_bs = FakeBaoStock()

        @contextmanager
        def fake_session():
            yield fake_bs

        with patch.object(baostock_adapter, "_session", fake_session):
            result = baostock_adapter.get_all_a_share_stocks()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["day"], "2026-03-13")
        self.assertEqual(
            result["data"],
            [
                {"code": "600000", "name": "浦发银行", "trade_status": "1", "bs_code": "sh.600000"},
                {"code": "000001", "name": "平安银行", "trade_status": "1", "bs_code": "sz.000001"},
            ],
        )

    def test_baostock_universe_prioritizes_core_index_members(self) -> None:
        class FakeBaoStock:
            def query_all_stock(self, day: str):
                return _FakeResultSet(
                    ["code", "tradeStatus", "code_name"],
                    [
                        ["sh.600999", "1", "招商证券"],
                        ["sh.600000", "1", "浦发银行"],
                        ["sh.688001", "1", "华兴源创"],
                        ["sz.000001", "1", "平安银行"],
                    ],
                )

            def query_trade_dates(self, start_date: str, end_date: str):
                return _FakeResultSet([ "calendar_date", "is_trading_day"], [["2026-03-13", "1"]])

            def query_sz50_stocks(self):
                return _FakeResultSet(["updateDate", "code", "code_name"], [["2026-03-16", "sh.600000", "浦发银行"]])

            def query_hs300_stocks(self):
                return _FakeResultSet(["updateDate", "code", "code_name"], [["2026-03-16", "sz.000001", "平安银行"]])

            def query_zz500_stocks(self):
                return _FakeResultSet(["updateDate", "code", "code_name"], [])

        fake_bs = FakeBaoStock()

        @contextmanager
        def fake_session():
            yield fake_bs

        with patch.object(baostock_adapter, "_session", fake_session):
            result = baostock_adapter.get_all_a_share_stocks("2026-03-13")

        self.assertEqual(
            [item["code"] for item in result["data"]],
            ["600000", "000001", "600999", "688001"],
        )


class UniversalGateTests(unittest.TestCase):
    def _scan_data(self, *, equity=1_000_000_000, profit=120_000_000, industry="煤炭") -> dict:
        return {
            "company_profile": {"data": {"行业": industry, "主营业务": f"{industry}主业", "实际控制人": "国务院国资委", "股票简称": "测试股份"}},
            "revenue_breakdown": {"data": [{"报告期": "20241231", "主营构成": industry, "主营收入": 85, "收入比例": 85}]},
            "valuation_history": {"data": {"pb": 0.95, "pb_percentile": 18}},
            "stock_kline": {"data": {"current_vs_5yr_high": 42, "latest_close": 10.0}},
            "realtime_quote": {"data": {"最新价": 10.0, "总市值": 8_000_000_000}},
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": profit}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": equity}]},
        }

    def test_negative_equity_triggers_hard_veto(self) -> None:
        gate = evaluate_universal_gates("000001", self._scan_data(equity=-10_000_000, profit=-5_000_000))
        self.assertTrue(gate["hard_vetos"])
        self.assertIn("balance sheet survival is questionable", gate["hard_vetos"])
        self.assertEqual(gate["scorecard"]["verdict"], "reject / no action")

    def test_quality_cyclical_case_scores_as_candidate(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertGreaterEqual(gate["scorecard"]["total"], 75)
        self.assertIn(
            gate["scorecard"]["verdict"],
            {"high conviction / strong candidate", "reasonable candidate / starter possible"},
        )


class PartialRadarFlowTests(unittest.TestCase):
    def _partial_scan_data_real_keys(self) -> dict[str, dict]:
        return {
            "company_profile": {
                "data": {
                    "行业": "煤炭",
                    "主营业务": "煤炭主业",
                    "实际控制人": "国务院国资委",
                    "股票简称": "测试股份",
                }
            },
            "revenue_breakdown": {
                "data": [
                    {"报告期": "20241231", "主营构成": "煤炭", "主营收入": 85, "收入比例": 85}
                ]
            },
            "valuation_history": {"data": {"pb": 0.95, "pb_percentile": 18}},
            "stock_kline": {"data": {"current_vs_5yr_high": 42, "latest_close": 10.0}},
            "realtime_quote": {"data": {"最新价": 10.0, "总市值": 8_000_000_000}},
        }

    def _full_scan_data_real_keys(self) -> dict[str, dict]:
        scan_data = self._partial_scan_data_real_keys()
        scan_data.update(
            {
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 120_000_000}]},
                "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 1_000_000_000}]},
            }
        )
        return scan_data

    def test_partial_gate_dimensions_include_confidence_and_requires(self) -> None:
        result = universal_gate_module.evaluate_partial_gate_dimensions("600348", self._partial_scan_data_real_keys())
        self.assertEqual(result["dimensions"]["survival"]["score"], 0.0)
        self.assertEqual(result["dimensions"]["survival"]["max"], 15.0)
        self.assertEqual(result["dimensions"]["survival"]["confidence"], "none")
        self.assertEqual(result["dimensions"]["survival"]["requires"], ["income_statement", "balance_sheet"])

    def test_partial_gate_scores_real_business_inputs_with_correct_keys(self) -> None:
        result = universal_gate_module.evaluate_partial_gate_dimensions("600348", self._partial_scan_data_real_keys())
        self.assertEqual(result["opportunity_context"]["primary_type"], "cyclical")
        self.assertEqual(result["opportunity_context"]["confidence"], "high")
        self.assertEqual(result["dimensions"]["business_quality"]["score"], 14.0)
        self.assertEqual(result["known_total"], 60.0)

    def test_prefilter_rejects_only_when_upper_bound_is_below_cutoff(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_gate = {"decidable_hard_vetos": [], "score_upper_bound": 64.99}
        self.assertTrue(radar_scan_engine._should_prefilter_reject(partial_gate, 65))

    def test_prefilter_advances_when_upper_bound_equals_cutoff(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_gate = {"decidable_hard_vetos": [], "score_upper_bound": 65.0}
        self.assertFalse(radar_scan_engine._should_prefilter_reject(partial_gate, 65))

    def test_partial_gate_separates_decidable_and_blocked_hard_vetos(self) -> None:
        scan_data = self._partial_scan_data_real_keys()
        scan_data["company_profile"] = {"data": {"行业": "", "主营业务": "", "股票简称": "测试股份"}}
        scan_data["revenue_breakdown"] = {"data": []}
        result = universal_gate_module.evaluate_partial_gate_dimensions("600348", scan_data)
        self.assertIn("business is not understandable", result["decidable_hard_vetos"])
        self.assertIn("normal earning power cannot be estimated", result["blocked_hard_vetos"])
        self.assertIn("balance sheet survival is questionable", result["blocked_hard_vetos"])

    def test_radar_enrichment_fetches_fields_from_requires(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_gate = {
            "dimensions": {
                "survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]},
                "management": {"confidence": "partial", "requires": ["company_profile"]},
                "valuation": {"confidence": "full", "requires": []},
            }
        }
        self.assertEqual(
            radar_scan_engine._fields_to_fetch_from_partial_gate(partial_gate),
            ["balance_sheet", "company_profile", "income_statement"],
        )

    def test_radar_path_does_not_fetch_financial_summary(self) -> None:
        self.assertNotIn("financial_summary", akshare_adapter.RADAR_PARTIAL_STEPS)
        self.assertNotIn("financial_summary", akshare_adapter.RADAR_EXPENSIVE_STEPS)

    def test_two_stage_radar_matches_existing_full_payload_for_survivor(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_scan_data = self._partial_scan_data_real_keys()
        enrichment_scan_data = {
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 120_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 1_000_000_000}]},
        }
        full_scan_data = self._full_scan_data_real_keys()
        expected_payload = radar_scan_engine._candidate_payload("600348", "测试股份", full_scan_data)
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 90.0,
            "dimensions": {
                "survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]},
                "valuation": {"confidence": "full", "requires": []},
            },
        }

        with patch.object(radar_scan_engine, "_load_universe", return_value=[{"code": "600348", "name": "测试股份"}]):
            with patch.object(radar_scan_engine, "run_full_scan", side_effect=AssertionError("legacy full scan path should not be used"), create=True):
                with patch.object(radar_scan_engine, "run_named_scan_steps", side_effect=[partial_scan_data, enrichment_scan_data], create=True):
                    with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                        with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                            result = radar_scan_engine.run_radar_scan("A-share", limit=1)

        ranked = result["priority_shortlist"] + result["secondary_watchlist"] + result["rejected"]
        self.assertEqual(ranked, [expected_payload])

    def test_two_stage_radar_preserves_baostock_universe_fallback(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_scan_data = self._partial_scan_data_real_keys()
        enrichment_scan_data = {
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 120_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 1_000_000_000}]},
        }
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 90.0,
            "dimensions": {
                "survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]},
                "valuation": {"confidence": "full", "requires": []},
            },
        }
        fallback_rows = {"data": [{"code": "600348", "name": "测试股份"}], "status": "ok_fallback_baostock_universe"}

        with patch.object(radar_scan_engine.ak, "stock_zh_a_spot_em", side_effect=RuntimeError("snapshot down")):
            with patch.object(radar_scan_engine, "get_all_a_share_stocks", return_value=fallback_rows, create=True):
                with patch.object(radar_scan_engine, "run_full_scan", side_effect=AssertionError("legacy full scan path should not be used"), create=True):
                    with patch.object(radar_scan_engine, "run_named_scan_steps", side_effect=[partial_scan_data, enrichment_scan_data], create=True):
                        with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                            with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                                result = radar_scan_engine.run_radar_scan("A-share", limit=1)

        self.assertEqual(result["universe_size"], 1)
        ranked = result["priority_shortlist"] + result["secondary_watchlist"] + result["rejected"]
        self.assertEqual(ranked[0]["ticker"], "600348")


class RadarDayCacheTests(unittest.TestCase):
    @staticmethod
    def _ok_result(field: str, data: dict[str, object]) -> dict[str, object]:
        return {
            "data": data,
            "evidence": {"field_name": field, "source_type": "akshare", "description": f"{field} fetched"},
            "status": "ok",
            "fetch_timestamp": "2026-03-16T10:00:00",
        }

    def test_run_named_scan_steps_prefers_memory_cache_before_day_cache(self) -> None:
        in_memory = {"company_profile": self._ok_result("company_profile", {"name": "memory-version"})}
        disk_cache = {"company_profile": self._ok_result("company_profile", {"name": "disk-version"})}

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            (cache_dir / "600328.json").write_text(json.dumps(disk_cache, ensure_ascii=False), encoding="utf-8")

            result = akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": lambda _: self._ok_result("company_profile", {"name": "network-version"})},
                cached_results=in_memory,
                day_cache_dir=cache_dir,
            )

        self.assertEqual(result["company_profile"]["data"]["name"], "memory-version")
        self.assertEqual(result["company_profile"]["status"], "ok")

    def test_run_named_scan_steps_writes_and_reuses_day_cache(self) -> None:
        fetch_count = {"company_profile": 0}

        def fetch_company_profile(_: str) -> dict[str, object]:
            fetch_count["company_profile"] += 1
            return self._ok_result("company_profile", {"name": "fresh-profile"})

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            first = akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": fetch_company_profile},
                day_cache_dir=cache_dir,
            )
            second = akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": fetch_company_profile},
                day_cache_dir=cache_dir,
            )
            cached_payload = json.loads((cache_dir / "600328.json").read_text(encoding="utf-8"))

        self.assertEqual(fetch_count["company_profile"], 1)
        self.assertEqual(first["company_profile"]["status"], "ok")
        self.assertEqual(second["company_profile"]["status"], "ok_day_cache")
        self.assertIn("company_profile", cached_payload)

    def test_day_cache_stage_two_append_preserves_stage_one_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": lambda _: self._ok_result("company_profile", {"name": "stage-one"})},
                day_cache_dir=cache_dir,
            )
            akshare_adapter.run_named_scan_steps(
                "600328",
                {"balance_sheet": lambda _: self._ok_result("balance_sheet", {"equity": 1000})},
                day_cache_dir=cache_dir,
            )
            cached_payload = json.loads((cache_dir / "600328.json").read_text(encoding="utf-8"))

        self.assertIn("company_profile", cached_payload)
        self.assertIn("balance_sheet", cached_payload)
        self.assertEqual(cached_payload["company_profile"]["data"]["name"], "stage-one")
        self.assertEqual(cached_payload["balance_sheet"]["data"]["equity"], 1000)

    def test_day_cache_misses_when_trade_date_changes(self) -> None:
        fetch_count = {"company_profile": 0}

        def fetch_company_profile(_: str) -> dict[str, object]:
            fetch_count["company_profile"] += 1
            return self._ok_result("company_profile", {"name": f"fetch-{fetch_count['company_profile']}"})

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            day_one = root / "2026-03-13"
            day_two = root / "2026-03-16"
            first = akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": fetch_company_profile},
                day_cache_dir=day_one,
            )
            second = akshare_adapter.run_named_scan_steps(
                "600328",
                {"company_profile": fetch_company_profile},
                day_cache_dir=day_two,
            )

        self.assertEqual(fetch_count["company_profile"], 2)
        self.assertEqual(first["company_profile"]["data"]["name"], "fetch-1")
        self.assertEqual(second["company_profile"]["data"]["name"], "fetch-2")

    def test_day_cache_hit_does_not_consult_hourly_staleness(self) -> None:
        disk_cache = {
            "company_profile": {
                **self._ok_result("company_profile", {"name": "cached-profile"}),
                "fetch_timestamp": "2026-03-01T09:00:00",
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            (cache_dir / "600328.json").write_text(json.dumps(disk_cache, ensure_ascii=False), encoding="utf-8")
            with patch.object(
                akshare_adapter,
                "_check_cache_freshness",
                side_effect=AssertionError("hourly staleness should not be consulted for day-cache hits"),
            ):
                result = akshare_adapter.run_named_scan_steps(
                    "600328",
                    {"company_profile": lambda _: self._ok_result("company_profile", {"name": "network-version"})},
                    day_cache_dir=cache_dir,
                )

        self.assertEqual(result["company_profile"]["status"], "ok_day_cache")
        self.assertEqual(result["company_profile"]["data"]["name"], "cached-profile")


class RadarParallelExecutionTests(unittest.TestCase):
    def _partial_scan_data(self) -> dict[str, dict]:
        return PartialRadarFlowTests()._partial_scan_data_real_keys()

    @staticmethod
    def _enrichment_scan_data() -> dict[str, dict]:
        return {
            "income_statement": {"data": [{"report_period": "20241231", "profit": 120_000_000}]},
            "balance_sheet": {"data": [{"report_period": "20241231", "equity": 1_000_000_000}]},
        }

    def test_resolve_radar_trade_date_falls_back_to_previous_weekday_when_remote_lookup_fails(self) -> None:
        with patch.object(
            akshare_adapter,
            "_load_trade_days_from_baostock",
            side_effect=RuntimeError("trade calendar unavailable"),
            create=True,
        ):
            trade_date = akshare_adapter.resolve_radar_trade_date(date(2026, 3, 15))

        self.assertEqual(trade_date, "2026-03-13")

    def test_run_radar_scan_initializes_day_cache_meta_once(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 60.0,
            "blocked_hard_vetos": [],
            "dimensions": {"survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]}},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            with patch.object(radar_scan_engine, "BASE_DIR", temp_root):
                with patch.object(radar_scan_engine, "_load_universe", return_value=[{"code": "600348", "name": "Test"}]):
                    with patch.object(radar_scan_engine, "resolve_radar_trade_date", return_value="2026-03-16", create=True):
                        with patch.object(radar_scan_engine, "run_named_scan_steps", return_value=self._partial_scan_data(), create=True):
                            with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                                with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                                    radar_scan_engine.run_radar_scan("A-share", limit=1)

            meta_path = temp_root / "data" / "processed" / "radar_cache" / "2026-03-16" / "_meta.json"
            self.assertTrue(meta_path.exists())

    def test_parallel_radar_matches_serial_result_set(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        universe = [
            {"code": "600348", "name": "Alpha"},
            {"code": "600328", "name": "Beta"},
        ]
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 90.0,
            "blocked_hard_vetos": [],
            "dimensions": {"survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]}},
        }

        def fake_scan_steps(stock_code: str, step_map: dict[str, object], **_: object) -> dict[str, dict]:
            if "income_statement" in step_map or "balance_sheet" in step_map:
                return self._enrichment_scan_data()
            data = self._partial_scan_data()
            data["realtime_quote"] = {"data": {"latest_price": 10.0, "total_market_cap": 8_000_000_000, "stock_code": stock_code}}
            return data

        def fake_candidate_payload(stock_code: str, company_name: str, _: dict[str, object]) -> dict[str, object]:
            score = 82.0 if stock_code == "600348" else 74.0
            return {
                "ticker": stock_code,
                "company_name": company_name,
                "score": score,
                "hard_veto": False,
                "reason": "ok",
            }

        with patch.object(radar_scan_engine, "_load_universe", return_value=universe):
            with patch.object(radar_scan_engine, "run_named_scan_steps", side_effect=fake_scan_steps, create=True):
                with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                    with patch.object(radar_scan_engine, "_candidate_payload", side_effect=fake_candidate_payload, create=True):
                        with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                            serial = radar_scan_engine.run_radar_scan("A-share", limit=2, max_workers_override=1)
                            parallel = radar_scan_engine.run_radar_scan("A-share", limit=2, max_workers_override=4)

        self.assertEqual(serial["ranked"], parallel["ranked"])

    def test_parallel_radar_preserves_universe_order_when_scores_tie(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        universe = [
            {"code": "600328", "name": "Alpha"},
            {"code": "600348", "name": "Beta"},
        ]
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 90.0,
            "blocked_hard_vetos": [],
            "dimensions": {"survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]}},
        }

        def fake_scan_steps(_: str, step_map: dict[str, object], **__: object) -> dict[str, dict]:
            if "income_statement" in step_map or "balance_sheet" in step_map:
                return self._enrichment_scan_data()
            return self._partial_scan_data()

        def fake_candidate_payload(stock_code: str, company_name: str, _: dict[str, object]) -> dict[str, object]:
            if stock_code == "600328":
                time.sleep(0.03)
            return {
                "ticker": stock_code,
                "company_name": company_name,
                "score": 50.08,
                "hard_veto": False,
                "reason": "ok",
            }

        with patch.object(radar_scan_engine, "_load_universe", return_value=universe):
            with patch.object(radar_scan_engine, "run_named_scan_steps", side_effect=fake_scan_steps, create=True):
                with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                    with patch.object(radar_scan_engine, "_candidate_payload", side_effect=fake_candidate_payload, create=True):
                        with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                            result = radar_scan_engine.run_radar_scan("600328,600348", limit=2, max_workers_override=4)

        self.assertEqual([item["ticker"] for item in result["ranked"]], ["600328", "600348"])

    def test_baostock_session_serializes_parallel_calls(self) -> None:
        class FakeBaoStock:
            def __init__(self) -> None:
                self.active_sessions = 0
                self.max_active_sessions = 0
                self.guard = threading.Lock()

            def login(self):
                with self.guard:
                    self.active_sessions += 1
                    self.max_active_sessions = max(self.max_active_sessions, self.active_sessions)
                time.sleep(0.03)
                return type("LoginResult", (), {"error_code": "0", "error_msg": "success"})()

            def logout(self):
                with self.guard:
                    self.active_sessions -= 1

            def query_history_k_data_plus(
                self,
                code: str,
                fields: str,
                start_date: str,
                end_date: str,
                frequency: str,
                adjustflag: str,
            ):
                time.sleep(0.03)
                row = [
                    end_date,
                    code,
                    "10.00",
                    "10.10",
                    "9.90",
                    "10.05",
                    "1000",
                    "10050.0",
                ]
                return _FakeResultSet(fields.split(","), [row])

        fake_bs = FakeBaoStock()
        threads: list[threading.Thread] = []

        with patch.object(baostock_adapter, "_load_baostock", return_value=fake_bs):
            for _ in range(3):
                thread = threading.Thread(
                    target=baostock_adapter.get_daily_history,
                    args=("600328", "2026-03-01", "2026-03-13", "date,code,open,high,low,close,volume,amount"),
                )
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()

        self.assertEqual(fake_bs.max_active_sessions, 1)


class ValuationAndReportTests(unittest.TestCase):
    def test_akshare_adapter_exports_path_for_main_entrypoint(self) -> None:
        self.assertTrue(hasattr(akshare_adapter, "Path"))

    def test_akshare_adapter_estimate_consolidation_months_handles_comma_values(self) -> None:
        months = akshare_adapter._estimate_consolidation_months(pd.Series(["1,234"]))
        self.assertEqual(months, 1)

    def _asset_play_scan_data(self) -> dict:
        return {
            "company_profile": {"data": {"行业": "银行", "主营业务": "商业银行业务", "实际控制人": "省国资委", "股票简称": "测试银行"}},
            "revenue_breakdown": {"data": [{"报告期": "20241231", "主营构成": "利息净收入", "主营收入": 90, "收入比例": 90}]},
            "valuation_history": {"data": {"pb": 0.72, "pb_percentile": 12}},
            "stock_kline": {"data": {"current_vs_5yr_high": 58, "latest_close": 8.0}},
            "realtime_quote": {"data": {"最新价": 8.0, "总市值": 40_000_000_000}},
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 3_000_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 50_000_000_000}]},
        }

    def test_asset_play_valuation_has_ordered_cases(self) -> None:
        scan_data = self._asset_play_scan_data()
        opportunity = determine_opportunity_type(
            "601000",
            scan_data["company_profile"]["data"],
            revenue_records=scan_data["revenue_breakdown"]["data"],
        )
        valuation = build_three_case_valuation("601000", scan_data, opportunity)
        self.assertLess(valuation["bear_case"]["implied_price"], valuation["base_case"]["implied_price"])
        self.assertLess(valuation["base_case"]["implied_price"], valuation["bull_case"]["implied_price"])

    def test_compounder_valuation_handles_missing_profit(self) -> None:
        valuation = build_three_case_valuation(
            "300001",
            {
                "company_profile": {"data": {"行业": "软件开发", "主营业务": "SaaS 订阅软件服务"}},
                "revenue_breakdown": {"data": []},
                "realtime_quote": {"data": {"最新价": 20.0, "总市值": 5_000_000_000}},
                "stock_kline": {"data": {"latest_close": 20.0}},
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司股东的净利润": None}]},
                "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司股东权益合计": 2_000_000_000}]},
            },
            {"primary_type": "compounder"},
        )
        self.assertIsNone(valuation["bear_case"]["implied_price"])
        self.assertIsNone(valuation["base_case"]["implied_price"])
        self.assertIsNone(valuation["bull_case"]["implied_price"])

    def test_turnaround_valuation_handles_negative_equity(self) -> None:
        valuation = build_three_case_valuation(
            "600004",
            {
                "company_profile": {"data": {"行业": "综合", "主营业务": "*ST 公司债务重组"}},
                "revenue_breakdown": {"data": []},
                "realtime_quote": {"data": {"最新价": 4.0, "总市值": 1_000_000_000}},
                "stock_kline": {"data": {"latest_close": 4.0}},
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司股东的净利润": None}]},
                "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司股东权益合计": -300_000_000}]},
            },
            {"primary_type": "turnaround"},
        )
        self.assertLess(valuation["bear_case"]["implied_equity_value"], 0)
        self.assertLess(valuation["base_case"]["implied_equity_value"], 0)
        self.assertLess(valuation["bull_case"]["implied_equity_value"], 0)

    def test_unknown_type_valuation_handles_missing_outcome_multiples(self) -> None:
        valuation = build_three_case_valuation(
            "600004",
            {
                "company_profile": {"data": {"行业": "综合", "主营业务": "暂无法归类"}},
                "revenue_breakdown": {"data": []},
                "realtime_quote": {"data": {"最新价": 12.0, "总市值": 1_200_000_000}},
                "stock_kline": {"data": {"latest_close": 12.0}},
                "income_statement": {"data": [{"报告期": "20241231", "归属于母公司股东的净利润": 50_000_000}]},
                "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司股东权益合计": 800_000_000}]},
            },
            {"primary_type": "unknown"},
        )
        self.assertIsNone(valuation["bear_case"]["implied_equity_value"])
        self.assertIsNone(valuation["base_case"]["implied_equity_value"])
        self.assertIsNone(valuation["bull_case"]["implied_equity_value"])

    def test_cyclical_valuation_handles_missing_haircuts_and_multiples(self) -> None:
        with patch("engines.valuation_engine.load_valuation_discipline", return_value={"opportunity_types": {"cyclical": {}}}):
            valuation = build_three_case_valuation(
                "600348",
                {
                    "company_profile": {"data": {"行业": "煤炭", "主营业务": "煤炭开采与销售"}},
                    "revenue_breakdown": {"data": []},
                    "realtime_quote": {"data": {"最新价": 12.0, "总市值": 1_200_000_000}},
                    "stock_kline": {"data": {"latest_close": 12.0}},
                    "income_statement": {"data": [{"报告期": "20241231", "归属于母公司股东的净利润": 50_000_000}]},
                    "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司股东权益合计": 800_000_000}]},
                },
                {"primary_type": "cyclical"},
            )
        self.assertIsNone(valuation["bear_case"]["implied_equity_value"])
        self.assertIsNone(valuation["base_case"]["implied_equity_value"])
        self.assertIsNone(valuation["bull_case"]["implied_equity_value"])

    def test_report_contains_required_sections(self) -> None:
        scan_data = self._asset_play_scan_data()
        opportunity = determine_opportunity_type(
            "601000",
            scan_data["company_profile"]["data"],
            revenue_records=scan_data["revenue_breakdown"]["data"],
        )
        gate = evaluate_universal_gates("601000", scan_data, opportunity_context=opportunity)
        valuation = build_three_case_valuation("601000", scan_data, opportunity)
        synthesis = build_investment_synthesis("601000", "测试银行", gate, valuation)
        with tempfile.TemporaryDirectory() as temp_dir:
            report = generate_deep_dive_report(
                "601000",
                "测试银行",
                market="A-share",
                scan_data=scan_data,
                gate_result=gate,
                valuation_result=valuation,
                synthesis_result=synthesis,
                report_dir=temp_dir,
            )
            content = Path(report["report_path"]).read_text(encoding="utf-8")
        for heading in [
            "## 1. Executive view",
            "## 8. Valuation truth",
            "## 12. Scorecard",
            "## 14. Bottom line",
        ]:
            self.assertIn(heading, content)

    def test_report_uses_configured_dimension_max_in_scorecard(self) -> None:
        gate_result = {
            "opportunity_context": {"primary_label": "Cyclical", "sentence": "Test thesis", "reason": "Test reason", "secondary_types": []},
            "scorecard": {
                "type_clarity": 5.0,
                "business_quality": 12.0,
                "survival": 10.0,
                "management": 7.0,
                "regime_cycle": 9.0,
                "valuation": 14.0,
                "catalyst": 4.0,
                "market_structure": 3.0,
                "total": 64.0,
                "verdict": "watch / needs work",
            },
            "signals": {"purity": {}, "moat": {}, "management": {}, "bottom_pattern": {}, "catalyst": {}},
            "gates": {"business_truth": {}, "survival_truth": {}, "quality_truth": {}, "regime_cycle_truth": {}},
            "hard_vetos": [],
        }
        valuation_result = {
            "current_price": 10.0,
            "bear_case": {"assumptions": [], "valuation_method": "PB", "implied_price": 8.0},
            "base_case": {"assumptions": [], "valuation_method": "PB", "implied_price": 10.0},
            "bull_case": {"assumptions": [], "valuation_method": "PB", "implied_price": 12.0},
            "summary": {"margin_of_safety": 0.15, "priced_in": "neutral"},
        }
        synthesis_result = {
            "market_perception": [],
            "what_market_misses": [],
            "why_gap_may_close": "",
            "anti_thesis": [],
            "falsification_points": [],
            "bottom_line": "Test bottom line",
        }
        scoring_rules = {
            "dimensions": {
                "opportunity_type_clarity": {"weight": 7},
                "business_quality": {"weight": 21},
                "survival_boundary": {"weight": 17},
                "management_capital_allocation": {"weight": 11},
                "regime_cycle_position": {"weight": 13},
                "valuation_margin_of_safety": {"weight": 19},
                "catalyst_value_realization": {"weight": 9},
                "market_structure_tradability": {"weight": 3},
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("engines.report_engine.load_scoring_rules", return_value=scoring_rules, create=True):
                report = generate_deep_dive_report(
                    "600348",
                    "Test Co",
                    market="A-share",
                    scan_data={"company_profile": {"data": {}}},
                    gate_result=gate_result,
                    valuation_result=valuation_result,
                    synthesis_result=synthesis_result,
                    report_dir=temp_dir,
                )
            content = Path(report["report_path"]).read_text(encoding="utf-8")

        self.assertIn("- type clarity: 5.0/7", content)
        self.assertIn("- business quality: 12.0/21", content)
        self.assertIn("- total: 64.0/100", content)


class LegacyCleanupTests(unittest.TestCase):
    def test_tier0_report_kind_uses_shared_helper(self) -> None:
        self.assertIs(tier0_verifier._report_kind, tier0_report_pack_adapter._report_kind)

    def test_tier0_autofill_main_avoids_json_load_open_pattern(self) -> None:
        source = (SHARED_DIR / "validators" / "tier0_autofill.py").read_text(encoding="utf-8")
        self.assertNotIn("json.load(open(", source)

    def test_research_utils_classify_state_ownership_is_safe_compat_alias(self) -> None:
        result = research_utils.classify_state_ownership("000001", "民营资本")
        self.assertEqual(result["category"], "private")
        self.assertEqual(result["score_impact"], 0)
        self.assertNotIn("gate_verdict", result)

    def test_status_to_plan_actual_is_not_exported(self) -> None:
        self.assertFalse(hasattr(research_utils, "status_to_plan_actual"))

    def test_legacy_crocodile_symbols_and_configs_are_removed(self) -> None:
        self.assertFalse(hasattr(research_utils, "determine_eco_context"))
        self.assertFalse(hasattr(research_utils, "load_crocodile_discipline"))
        self.assertFalse(hasattr(research_utils, "get_crocodile_mode_config"))
        config_dir = SHARED_DIR / "config"
        self.assertFalse((config_dir / "crocodile_discipline.yaml").exists())
        self.assertFalse((config_dir / "industry_mapping.yaml").exists())

    def test_signal_health_prefers_primary_type_to_legacy_mode_names(self) -> None:
        result = evaluate_signal_health_v2(
            {"primary_type": "cyclical", "four_signal_mode": "shovel_play"},
            {
                "field_map": {
                    "spot_price": {"status": "ok"},
                    "industry_inventory": {"status": "ok"},
                    "pb_ratio": {"status": "ok"},
                    "capex_investment": {"status": "ok"},
                },
                "summary": {"stale_fields": []},
            },
            {
                "futures": {"status": "ok"},
                "inventory": {"data": {"coverage": "exchange_only"}},
                "exchange_inventory": {"status": "ok"},
                "social_inventory": {"status": "missing"},
            },
            {"industry_fai": {"status": "ok"}},
        )
        self.assertEqual(result["mode"], "cyclical")
        self.assertEqual(result["core_names"], ["price_signal", "inventory_signal", "capex_signal"])
        self.assertIn("inventory_exchange_only", result["coverage_warnings"])

    def test_legacy_skill_tree_is_archived_outside_active_skills(self) -> None:
        self.assertFalse((SHARED_DIR.parents[1] / "skills" / "a_stock_sniper").exists())
        self.assertTrue((SHARED_DIR.parents[1] / "_archive" / "a_stock_sniper").exists())

    def test_business_simplicity_uses_opportunity_type_templates(self) -> None:
        result = evaluate_business_simplicity(
            "cyclical",
            {
                "items": [
                    {
                        "field_name": "cost_structure",
                        "candidate_value": {
                            "summary": "verified cost structure",
                            "semantic_check": {"semantic_pass": True},
                        },
                    }
                ]
            },
        )
        self.assertEqual(result["formula"], "(unit price - unit cost) * normalized volume")
        self.assertEqual(result["status"], "pass")


# ---------------------------------------------------------------------------
# Chunk 1 – VCRF failing tests (flow engine, valuation contract, gate/radar)
# These tests are expected to FAIL until the corresponding engines are built.
# ---------------------------------------------------------------------------


class VCRFFlowEngineTests(unittest.TestCase):
    """Task 1: Flow/realization engine contract tests."""

    def test_flow_engine_classifies_ignition_when_turnover_and_relative_strength_improve(self) -> None:
        from engines.flow_realization_engine import FlowInputs, score_flow_setup

        result = score_flow_setup(
            FlowInputs(
                current_price=10.0,
                avg20_turnover=1.8,
                avg120_turnover=1.0,
                rel_strength_20d=0.09,
                rel_strength_60d=0.12,
                rebound_from_low_pct=0.18,
                shareholder_concentration_delta=0.0,
                institutional_holding_delta=0.0,
                buyback_flag=False,
                mna_flag=False,
            )
        )
        self.assertEqual(result["stage"], "ignition")

    def test_position_state_is_cold_storage_when_floor_is_strong_but_flow_is_latent(self) -> None:
        from engines.flow_realization_engine import classify_position_state

        state = classify_position_state(
            floor_protection=0.92,
            normalized_upside=0.45,
            recognition_upside=0.70,
            repair_state="stabilizing",
            flow_stage="latent",
        )
        self.assertEqual(state, "cold_storage")

    def test_position_state_is_attack_when_flow_trends_and_recognition_upside_remains(self) -> None:
        from engines.flow_realization_engine import classify_position_state

        state = classify_position_state(
            floor_protection=0.88,
            normalized_upside=0.35,
            recognition_upside=0.55,
            repair_state="confirmed",
            flow_stage="trend",
        )
        self.assertEqual(state, "attack")

    def test_flow_engine_degrades_to_latent_when_optional_inputs_are_missing(self) -> None:
        from engines.flow_realization_engine import FlowInputs, score_flow_setup

        result = score_flow_setup(
            FlowInputs(
                current_price=10.0,
                avg20_turnover=None,
                avg120_turnover=None,
                rel_strength_20d=None,
                rel_strength_60d=None,
                rebound_from_low_pct=None,
                shareholder_concentration_delta=None,
                institutional_holding_delta=None,
            )
        )
        self.assertIn(result["stage"], {"abandoned", "latent"})

    def test_position_state_rejects_when_floor_protection_is_too_low(self) -> None:
        from engines.flow_realization_engine import classify_position_state

        state = classify_position_state(
            floor_protection=0.60,
            normalized_upside=0.50,
            recognition_upside=0.80,
            repair_state="confirmed",
            flow_stage="trend",
        )
        self.assertEqual(state, "reject")

    def test_position_state_is_harvest_when_normalized_upside_exhausted_in_trend(self) -> None:
        from engines.flow_realization_engine import classify_position_state

        state = classify_position_state(
            floor_protection=0.95,
            normalized_upside=0.10,
            recognition_upside=0.15,
            repair_state="confirmed",
            flow_stage="trend",
        )
        self.assertEqual(state, "harvest")

    def test_flow_stage_order_is_monotonic(self) -> None:
        from engines.flow_realization_engine import FLOW_STAGE_ORDER

        ordered = sorted(FLOW_STAGE_ORDER.items(), key=lambda kv: kv[1])
        self.assertEqual(
            [stage for stage, _ in ordered],
            ["abandoned", "latent", "ignition", "trend", "crowded"],
        )

    def test_secondary_watch_collapses_into_cold_storage(self) -> None:
        """Phase-1 rule: secondary_watch is not a legal state."""
        from engines.flow_realization_engine import classify_position_state

        # Latent flow + floor OK but normalized_upside below cold_storage threshold
        state = classify_position_state(
            floor_protection=0.85,
            normalized_upside=0.30,
            recognition_upside=0.50,
            repair_state="none",
            flow_stage="latent",
        )
        self.assertIn(state, {"cold_storage", "reject"})
        self.assertNotEqual(state, "secondary_watch")


class VCRFValuationContractTests(unittest.TestCase):
    """Task 2: Valuation engine VCRF output contract tests."""

    def _cyclical_scan_data(self) -> dict:
        return {
            "company_profile": {"data": {"行业": "煤炭", "主营业务": "煤炭开采与销售"}},
            "revenue_breakdown": {"data": [{"报告期": "20241231", "主营构成": "煤炭", "主营收入": 85, "收入比例": 85}]},
            "valuation_history": {"data": {"pb": 0.95, "pb_percentile": 18}},
            "stock_kline": {"data": {"current_vs_5yr_high": 42, "latest_close": 10.0}},
            "realtime_quote": {"data": {"最新价": 10.0, "总市值": 8_000_000_000}},
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 120_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 1_000_000_000}]},
        }

    def test_valuation_outputs_vcrf_case_names_and_summary_metrics(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        self.assertIn("floor_case", valuation)
        self.assertIn("normalized_case", valuation)
        self.assertIn("recognition_case", valuation)
        self.assertIn("floor_protection", valuation["summary"])
        self.assertIn("normalized_upside", valuation["summary"])
        self.assertIn("recognition_upside", valuation["summary"])
        self.assertIn("wind_dependency", valuation["summary"])

    def test_valuation_keeps_base_case_alias_during_transition(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        self.assertEqual(
            valuation["base_case"]["implied_price"],
            valuation["normalized_case"]["implied_price"],
        )

    def test_valuation_keeps_bear_case_alias_during_transition(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        self.assertEqual(
            valuation["bear_case"]["implied_price"],
            valuation["floor_case"]["implied_price"],
        )

    def test_valuation_keeps_bull_case_alias_during_transition(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        self.assertEqual(
            valuation["bull_case"]["implied_price"],
            valuation["recognition_case"]["implied_price"],
        )

    def test_floor_protection_is_ratio_of_floor_to_current_price(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        if valuation["floor_case"]["implied_price"] is not None:
            expected = valuation["floor_case"]["implied_price"] / valuation["current_price"]
            self.assertAlmostEqual(valuation["summary"]["floor_protection"], expected, places=4)

    def test_normalized_upside_is_ratio_minus_one(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        if valuation["normalized_case"]["implied_price"] is not None:
            expected = valuation["normalized_case"]["implied_price"] / valuation["current_price"] - 1
            self.assertAlmostEqual(valuation["summary"]["normalized_upside"], expected, places=4)

    def test_cyclical_normalized_value_prefers_history_based_anchor(self) -> None:
        scan_data = self._cyclical_scan_data()
        valuation = build_three_case_valuation("600348", scan_data, {"primary_type": "cyclical"})
        if valuation["normalized_case"]["implied_equity_value"] is not None and valuation["floor_case"]["implied_equity_value"] is not None:
            self.assertGreater(
                valuation["normalized_case"]["implied_equity_value"],
                valuation["floor_case"]["implied_equity_value"],
            )


class VCRFGateAndRadarTests(unittest.TestCase):
    """Task 3: Gate-state and radar behavior contract tests."""

    def _scan_data(self, *, equity=1_000_000_000, profit=120_000_000, industry="煤炭") -> dict:
        return {
            "company_profile": {"data": {"行业": industry, "主营业务": f"{industry}主业", "实际控制人": "国务院国资委", "股票简称": "测试股份"}},
            "revenue_breakdown": {"data": [{"报告期": "20241231", "主营构成": industry, "主营收入": 85, "收入比例": 85}]},
            "valuation_history": {"data": {"pb": 0.95, "pb_percentile": 18}},
            "stock_kline": {"data": {"current_vs_5yr_high": 42, "latest_close": 10.0}},
            "realtime_quote": {"data": {"最新价": 10.0, "总市值": 8_000_000_000}},
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": profit}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": equity}]},
        }

    def test_universal_gate_outputs_vcrf_gate_names(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn("business_or_asset_truth", gate["gates"])
        self.assertIn("governance_truth", gate["gates"])
        self.assertIn("valuation_floor_truth", gate["gates"])
        self.assertIn("realization_truth", gate["gates"])

    def test_universal_gate_includes_hidden_position_state(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn(gate["position_state"], {"cold_storage", "ready", "attack", "harvest", "reject"})

    def test_universal_gate_includes_flow_stage(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn(gate["flow_stage"], {"abandoned", "latent", "ignition", "trend", "crowded"})

    def test_universal_gate_preserves_signals_catalyst_bridge(self) -> None:
        """Phase-1 compatibility: signals.catalyst must still exist for synthesis_engine."""
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn("catalyst", gate.get("signals", {}))

    def test_universal_gate_preserves_legacy_gate_aliases(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        # Legacy consumers may still read these
        self.assertIn("business_truth", gate["gates"])
        self.assertIn("survival_truth", gate["gates"])

    def test_radar_priority_bucket_requires_ready_or_attack_state(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        partial_scan_data = PartialRadarFlowTests()._partial_scan_data_real_keys()
        enrichment_scan_data = {
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": 120_000_000}]},
            "balance_sheet": {"data": [{"报告期": "20241231", "归属于母公司所有者权益合计": 1_000_000_000}]},
        }
        partial_gate = {
            "decidable_hard_vetos": [],
            "score_upper_bound": 90.0,
            "blocked_hard_vetos": [],
            "dimensions": {
                "survival": {"confidence": "none", "requires": ["income_statement", "balance_sheet"]},
                "valuation": {"confidence": "full", "requires": []},
            },
        }

        with patch.object(radar_scan_engine, "_load_universe", return_value=[{"code": "600348", "name": "测试股份"}]):
            with patch.object(radar_scan_engine, "run_named_scan_steps", side_effect=[partial_scan_data, enrichment_scan_data], create=True):
                with patch.object(radar_scan_engine, "evaluate_partial_gate_dimensions", return_value=partial_gate, create=True):
                    with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                        result = radar_scan_engine.run_radar_scan("A-share", limit=1)

        for item in result.get("priority_shortlist", []):
            self.assertIn(
                item.get("position_state"),
                {"ready", "attack"},
                f"priority_shortlist should only contain ready/attack, got {item.get('position_state')}",
            )

    def test_radar_candidate_payload_includes_vcrf_fields(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        scan_data = self._scan_data()
        payload = radar_scan_engine._candidate_payload("600348", "测试股份", scan_data)
        for field in ("position_state", "flow_stage", "floor_protection", "normalized_upside", "recognition_upside"):
            self.assertIn(field, payload, f"candidate payload missing VCRF field: {field}")

    def test_load_universe_prefers_layered_sample_over_top_market_cap_slice(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        # Build a fake snapshot with clear size diversity
        rows = []
        for i, (code, name, cap) in enumerate([
            ("600000", "MegaCap", 500_000_000_000),
            ("600001", "LargeCap", 100_000_000_000),
            ("600002", "MidCap1", 30_000_000_000),
            ("600003", "MidCap2", 20_000_000_000),
            ("600004", "SmallCap1", 8_000_000_000),
            ("600005", "SmallCap2", 6_000_000_000),
            ("600006", "SmallCap3", 5_500_000_000),
            ("600007", "MicroCap", 3_000_000_000),
        ]):
            rows.append({"代码": code, "名称": name, "总市值": cap})
        fake_df = pd.DataFrame(rows)

        with patch.object(radar_scan_engine.ak, "stock_zh_a_spot_em", return_value=fake_df):
            universe = radar_scan_engine._load_universe("A-share", 6)

        codes = [item["code"] for item in universe]
        # Old behavior would just take the top-6 by market cap.
        # New behavior must include at least one mid or small cap name.
        top_6_by_cap = ["600000", "600001", "600002", "600003", "600004", "600005"]
        self.assertNotEqual(codes, top_6_by_cap, "Universe should not be pure top-cap slice")


class VCRFUniversalGateTests(unittest.TestCase):
    def _scan_data(self, *, equity=1_000_000_000, profit=120_000_000, industry="煤炭") -> dict:
        return {
            "company_profile": {"data": {"行业": industry, "主营业务": f"{industry}主业", "实际控制人": "国务院国资委", "股票简称": "测试股份"}},
            "revenue_breakdown": {"data": [{"报告期": "20241231", "主营构成": industry, "主营收入": 85, "收入比例": 85}]},
            "valuation_history": {"data": {"pb": 0.95, "pb_percentile": 18}},
            "stock_kline": {
                "data": {
                    "current_vs_5yr_high": 42,
                    "latest_close": 10.0,
                    "volume_ratio_20_vs_120": 1.4,
                    "drawdown_from_5yr_high_pct": 50,
                    "low_5y": 8.0,
                    "avg_turnover_1y": 500_000_000,
                }
            },
            "realtime_quote": {"data": {"最新价": 10.0, "总市值": 8_000_000_000}},
            "income_statement": {"data": [{"报告期": "20241231", "归属于母公司所有者的净利润": profit}]},
            "balance_sheet": {
                "data": [
                    {
                        "报告期": "20241231",
                        "归属于母公司所有者权益合计": equity,
                        "资产总计": 2_000_000_000,
                        "货币资金": 600_000_000,
                        "短期借款": 150_000_000,
                    }
                ]
            },
            "cashflow_statement": {"data": [{"报告期": "20241231", "经营活动产生的现金流量净额": 260_000_000}]},
            "shareholder_count": {"data": [{"股东户数": 120_000}, {"股东户数": 135_000}]},
            "event_signals": {"buyback": True},
        }

    def test_universal_gate_returns_driver_stack_and_dual_axes(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn("driver_stack", gate)
        self.assertIn("underwrite_axis", gate)
        self.assertIn("realization_axis", gate)

    def test_universal_gate_keeps_legacy_scorecard_aliases_during_transition(self) -> None:
        gate = evaluate_universal_gates("600348", self._scan_data())
        self.assertIn("scorecard", gate)
        self.assertIn("business_truth", gate["gates"])

    def test_harvest_to_attack_is_downgraded(self) -> None:
        aggressive_scan_data = self._scan_data(profit=240_000_000)
        gate = evaluate_universal_gates("600348", aggressive_scan_data, prior_state="HARVEST")
        self.assertNotEqual(gate["position_state"], "attack")


class VCRFRadarIntegrationTests(unittest.TestCase):
    def test_radar_coarse_stage_limits_fine_stage_candidate_count(self) -> None:
        radar_scan_engine = _load_radar_scan_engine()
        with patch.object(radar_scan_engine, "_load_universe", return_value=[{"code": "600348", "name": "测试股份"}]):
            with patch.object(
                radar_scan_engine,
                "_coarse_filter_universe",
                return_value=[{"code": "600348", "name": "测试股份"}],
                create=True,
            ):
                with patch.object(
                    radar_scan_engine,
                    "_scan_one_stock",
                    return_value={
                        "kind": "ranked",
                        "order_index": 0,
                        "payload": {
                            "ticker": "600348",
                            "company_name": "测试股份",
                            "opportunity_type": "Cyclical",
                            "score": 78.0,
                            "hard_veto": False,
                            "position_state": "ready",
                            "thesis": "Test thesis",
                            "mispricing": "base case 12 vs current 10",
                            "catalysts": [],
                            "risks": [],
                            "why_passed": "Test reason",
                            "next_step": "deep dive now",
                            "reason": "N/A",
                        },
                    },
                    create=True,
                ):
                    with patch.object(radar_scan_engine, "generate_market_scan_report", return_value={"report_path": "report.md"}, create=True):
                        result = radar_scan_engine.run_radar_scan("A-share", limit=24)
        self.assertIn("coarse_candidate_count", result)
        self.assertLessEqual(result["fine_candidate_count"], result["coarse_candidate_count"])

    def test_vcrf_calibrator_reports_axis_quantiles(self) -> None:
        from engines.vcrf_calibrator import summarize_axis_distribution

        report = summarize_axis_distribution([10, 20, 30, 40, 50])
        self.assertIn("p50", report)
        self.assertIn("histogram", report)


class VCRFConfigContractTests(unittest.TestCase):
    def test_loaders_expose_vcrf_config_files(self) -> None:
        from utils.config_loader import (
            load_vcrf_degradation,
            load_vcrf_state_machine,
            load_vcrf_weights,
        )

        self.assertIn("base_templates", load_vcrf_weights())
        self.assertIn("allowed_transitions", load_vcrf_state_machine())
        self.assertIn("degradation_rules", load_vcrf_degradation())

    def test_all_weight_templates_normalize_after_sector_overrides(self) -> None:
        from utils.config_loader import resolve_vcrf_weight_template

        primary_types = ["compounder", "cyclical", "turnaround", "asset_play", "special_situation"]
        routes = ["core_resource", "rigid_shovel", "core_military", "financial_asset", "consumer", "tech", "unknown"]
        for primary_type in primary_types:
            for route in routes:
                template = resolve_vcrf_weight_template(primary_type, route)
                self.assertAlmostEqual(sum(template["underwrite"].values()), 1.0, places=3)
                self.assertAlmostEqual(sum(template["realization"].values()), 1.0, places=3)

    def test_state_machine_config_includes_new_state_and_harvest_candidate_rules(self) -> None:
        from utils.config_loader import load_vcrf_state_machine

        cfg = load_vcrf_state_machine()
        self.assertIn("NEW", cfg["allowed_transitions"])
        self.assertIn("harvest_candidate", cfg)
        self.assertEqual(cfg["harvest_candidate"]["consecutive_closes_above_recognition"], 3)


class VCRFDataSourceTests(unittest.TestCase):
    def test_akshare_adapter_exposes_cashflow_statement_step(self) -> None:
        from adapters.akshare_adapter import RADAR_ALL_STEPS

        self.assertIn("cashflow_statement", RADAR_ALL_STEPS)

    def test_akshare_adapter_exposes_shareholder_count_step(self) -> None:
        from adapters.akshare_adapter import RADAR_PARTIAL_STEPS

        self.assertIn("shareholder_count", RADAR_PARTIAL_STEPS)

    def test_cninfo_adapter_exposes_vcrf_event_query(self) -> None:
        from adapters.cninfo_adapter import fetch_vcrf_event_signals

        result = fetch_vcrf_event_signals("600348", start_date="20240101", end_date="20241231")
        self.assertIn("events", result)


class VCRFStateHistoryTests(unittest.TestCase):
    def test_missing_history_resolves_to_new_state(self) -> None:
        from engines.state_transition_tracker import load_latest_state

        self.assertEqual(load_latest_state("600348", history_path="missing.jsonl"), "NEW")

    def test_forbidden_transition_is_downgraded(self) -> None:
        from engines.state_transition_tracker import enforce_transition
        from utils.config_loader import load_vcrf_state_machine

        state, allowed, reason = enforce_transition("HARVEST", "ATTACK", cfg=load_vcrf_state_machine())
        self.assertEqual(state, "COLD_STORAGE")
        self.assertFalse(allowed)
        self.assertTrue(reason)


class VCRFDriverStackTests(unittest.TestCase):
    def test_sector_route_resolves_from_active_sector_classification(self) -> None:
        from utils.primary_type_router import resolve_sector_route

        route = resolve_sector_route(
            "600348",
            {"行业": "煤炭", "主营业务": "煤炭开采与销售"},
            revenue_records=[{"主营构成": "煤炭", "主营收入": 85}],
        )
        self.assertEqual(route["sector_route"], "core_resource")

    def test_turnaround_routing_wins_when_losses_and_repair_evidence_exist(self) -> None:
        from utils.primary_type_router import determine_primary_type

        primary_type, confidence = determine_primary_type(
            sector_route="core_resource",
            preliminary_cycle_state="repair",
            financials_3y={"losses_2y": True, "repair_evidence": True},
            tags=[],
            events={},
            big_bath_result={"verdict": "inconclusive"},
        )
        self.assertEqual(primary_type, "turnaround")
        self.assertGreaterEqual(confidence, 0.75)

    def test_missing_survival_boundary_caps_state_at_cold_storage(self) -> None:
        from validators.universal_gate import _apply_degradation_caps

        adjusted = _apply_degradation_caps(
            proposed_state="ATTACK",
            component_availability={"survival_boundary": "missing"},
        )
        self.assertEqual(adjusted, "COLD_STORAGE")


class VCRFRealizationEngineTests(unittest.TestCase):
    def test_realization_axis_returns_all_six_components(self) -> None:
        from engines.flow_realization_engine import score_realization_axis

        result = score_realization_axis(scan_data={}, driver_stack={})
        self.assertEqual(
            set(result["components"].keys()),
            {
                "repair_state",
                "regime_cycle_position",
                "marginal_buyer_probability",
                "flow_confirmation",
                "elasticity",
                "catalyst_quality",
            },
        )

    def test_attack_book_monitor_requires_price_and_flow_confirmation(self) -> None:
        from engines.attack_book_monitor import evaluate_harvest_candidate

        result = evaluate_harvest_candidate(
            closes=[10.1, 10.2, 10.3],
            recognition_price=10.0,
            daily_returns=[0.01, 0.01, 0.01],
            flow_stage="trend",
            cfg={"consecutive_closes_above_recognition": 3, "require_flow_stage_deterioration_to": "crowded"},
        )
        self.assertFalse(result["harvest_candidate"])


class VCRFValuationRouteTests(unittest.TestCase):
    def test_normalized_case_depends_on_sector_route_not_primary_type_only(self) -> None:
        scan_data = VCRFValuationContractTests()._cyclical_scan_data()
        valuation = build_three_case_valuation(
            "600348",
            scan_data,
            {"sector_route": "core_resource", "primary_type": "cyclical"},
        )
        self.assertEqual(valuation["route_anchor"], "core_resource_mid_cycle")


if __name__ == "__main__":
    unittest.main()
