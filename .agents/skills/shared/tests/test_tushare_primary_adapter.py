import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

from adapters import tushare_adapter


class TusharePrimaryAdapterTests(unittest.TestCase):
    def test_get_company_profile_supports_hk_basic(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_hk_basic",
            return_value={
                "data": [
                    {
                        "ts_code": "09899.HK",
                        "name": "网易云音乐",
                        "fullname": "网易云音乐股份有限公司",
                        "enname": "NetEase Cloud Music Inc.",
                        "market": "主板",
                        "list_status": "L",
                        "list_date": "20211202",
                        "delist_date": "",
                        "trade_unit": 50.0,
                        "curr_type": "HKD",
                    }
                ],
                "status": "ok",
            },
        ):
            result = tushare_adapter.get_company_profile("09899")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["data"]["股票代码"], "09899")
        self.assertEqual(result["data"]["股票简称"], "网易云音乐")
        self.assertEqual(result["data"]["公司名称"], "网易云音乐股份有限公司")
        self.assertEqual(result["data"]["英文名称"], "NetEase Cloud Music Inc.")
        self.assertEqual(result["data"]["市场类型"], "主板")
        self.assertEqual(result["data"]["上市时间"], "20211202")
        self.assertEqual(result["evidence"]["source_type"], "tushare")

    def test_get_company_profile_prefers_tushare_and_normalizes_fields(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_stock_basic",
            return_value={
                "data": [
                    {
                        "ts_code": "600328.SH",
                        "symbol": "600328",
                        "name": "中盐化工",
                        "area": "内蒙古",
                        "industry": "基础化工",
                        "list_date": "20001222",
                        "delist_date": "",
                    }
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_stock_company",
            return_value={
                "data": [
                    {
                        "province": "内蒙古",
                        "introduction": "公司介绍",
                        "business_scope": "纯碱、烧碱、PVC",
                        "main_business": "纯碱和氯碱业务",
                    }
                ],
                "status": "ok",
            },
        ):
            result = tushare_adapter.get_company_profile("600328")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["data"]["股票代码"], "600328")
        self.assertEqual(result["data"]["股票简称"], "中盐化工")
        self.assertEqual(result["data"]["行业"], "基础化工")
        self.assertEqual(result["data"]["主营业务"], "纯碱和氯碱业务")
        self.assertEqual(result["evidence"]["source_type"], "tushare")

    def test_get_company_profile_falls_back_to_akshare_when_tushare_has_no_rows(self) -> None:
        fallback = {
            "data": {"股票代码": "600328", "股票简称": "中盐化工", "行业": "化工"},
            "evidence": {"source_type": "akshare"},
            "status": "ok",
            "fetch_timestamp": "2026-03-18T10:00:00",
        }
        with patch("adapters.tushare_adapter.query_stock_basic", return_value={"data": [], "status": "ok"}), patch(
            "adapters.tushare_adapter.query_stock_company",
            return_value={"data": [], "status": "ok"},
        ), patch(
            "adapters.akshare_adapter.get_company_profile",
            return_value=fallback,
        ):
            result = tushare_adapter.get_company_profile("600328")

        self.assertEqual(result["data"]["股票简称"], "中盐化工")
        self.assertEqual(result["evidence"]["source_type"], "akshare")
        self.assertEqual(result["status"], "ok_fallback_akshare")

    def test_get_realtime_quote_builds_delayed_snapshot_from_tushare_daily_rows(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_daily",
            return_value={
                "data": [
                    {"trade_date": "20260316", "close": 10.2},
                    {"trade_date": "20260317", "close": 10.8},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_daily_basic",
            return_value={
                "data": [
                    {"trade_date": "20260316", "total_mv": 100, "circ_mv": 80, "pb": 1.1},
                    {"trade_date": "20260317", "total_mv": 110, "circ_mv": 88, "pb": 1.2},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.get_company_profile",
            return_value={
                "data": {"股票代码": "600328", "股票简称": "中盐化工", "行业": "基础化工"},
                "status": "ok",
                "evidence": {"source_type": "tushare"},
                "fetch_timestamp": "2026-03-18T10:00:00",
            },
        ):
            result = tushare_adapter.get_realtime_quote("600328")

        self.assertEqual(result["status"], "ok_tushare_daily_snapshot")
        self.assertEqual(result["data"]["代码"], "600328")
        self.assertEqual(result["data"]["股票简称"], "中盐化工")
        self.assertAlmostEqual(result["data"]["最新价"], 10.8, places=4)
        self.assertAlmostEqual(result["data"]["总市值"], 1_100_000.0, places=4)
        self.assertEqual(result["data"]["最新交易日"], "2026-03-17")
        self.assertEqual(result["evidence"]["source_type"], "tushare")

    def test_get_realtime_quote_supports_us_daily_snapshot(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_us_daily",
            return_value={
                "data": [
                    {"trade_date": "20260316", "close": 252.82},
                    {"trade_date": "20260317", "close": 254.23},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.get_company_profile",
            return_value={
                "data": {"股票代码": "AAPL", "股票简称": "APPLE", "市场类型": "US"},
                "status": "ok",
                "evidence": {"source_type": "tushare"},
                "fetch_timestamp": "2026-03-18T10:00:00",
            },
        ):
            result = tushare_adapter.get_realtime_quote("AAPL")

        self.assertEqual(result["status"], "ok_tushare_daily_snapshot")
        self.assertEqual(result["data"]["代码"], "AAPL")
        self.assertEqual(result["data"]["股票简称"], "APPLE")
        self.assertAlmostEqual(result["data"]["最新价"], 254.23, places=4)
        self.assertEqual(result["data"]["最新交易日"], "2026-03-17")
        self.assertIsNone(result["data"]["总市值"])
        self.assertEqual(result["evidence"]["source_type"], "tushare")

    def test_get_all_a_share_stocks_enriches_listing_rows_with_trade_day_snapshot(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_stock_basic",
            return_value={
                "data": [
                    {"ts_code": "600328.SH", "symbol": "600328", "name": "中盐化工", "industry": "基础化工"},
                    {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "industry": "银行"},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_daily_basic",
            return_value={
                "data": [
                    {"ts_code": "600328.SH", "trade_date": "20260317", "total_mv": 110, "circ_mv": 88},
                    {"ts_code": "000001.SZ", "trade_date": "20260317", "total_mv": 2_000, "circ_mv": 1_500},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_daily",
            return_value={
                "data": [
                    {"ts_code": "600328.SH", "trade_date": "20260317", "amount": 350_000},
                    {"ts_code": "000001.SZ", "trade_date": "20260317", "amount": 9_000_000},
                ],
                "status": "ok",
            },
        ):
            result = tushare_adapter.get_all_a_share_stocks(day="2026-03-17")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["day"], "2026-03-17")
        self.assertEqual(result["data"][0]["code"], "000001")
        self.assertEqual(result["data"][0]["industry"], "银行")
        self.assertAlmostEqual(result["data"][0]["market_cap"], 20_000_000.0, places=4)
        self.assertAlmostEqual(result["data"][0]["turnover"], 9_000_000_000.0, places=4)

    def test_get_all_a_share_stocks_falls_back_to_previous_trade_day_when_today_snapshot_is_empty(self) -> None:
        with patch(
            "adapters.tushare_adapter.query_stock_basic",
            return_value={
                "data": [
                    {"ts_code": "600328.SH", "symbol": "600328", "name": "中盐化工", "industry": "基础化工"},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_trade_cal",
            return_value={
                "data": [
                    {"cal_date": "20260317", "is_open": "1"},
                    {"cal_date": "20260318", "is_open": "1"},
                ],
                "status": "ok",
            },
        ), patch(
            "adapters.tushare_adapter.query_daily_basic",
            side_effect=[
                {"data": [], "status": "ok"},
                {"data": [{"ts_code": "600328.SH", "trade_date": "20260317", "total_mv": 110, "circ_mv": 88}], "status": "ok"},
            ],
        ), patch(
            "adapters.tushare_adapter.query_daily",
            side_effect=[
                {"data": [], "status": "ok"},
                {"data": [{"ts_code": "600328.SH", "trade_date": "20260317", "amount": 350_000}], "status": "ok"},
            ],
        ):
            result = tushare_adapter.get_all_a_share_stocks(day="2026-03-18")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["day"], "2026-03-17")
        self.assertAlmostEqual(result["data"][0]["market_cap"], 1_100_000.0, places=4)


if __name__ == "__main__":
    unittest.main()
