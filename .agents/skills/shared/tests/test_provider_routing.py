import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

RADAR_ENGINE_PATH = SHARED_DIR.parent / "market-opportunity-scanner" / "scripts" / "engines" / "radar_scan_engine.py"
DEEP_DIVE_ENGINE_PATH = SHARED_DIR.parent / "single-stock-deep-dive" / "scripts" / "engines" / "deep_sniper_engine.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class ProviderRoutingTests(unittest.TestCase):
    def test_provider_router_defaults_to_tushare(self) -> None:
        from adapters import provider_router

        self.assertEqual(provider_router.get_scan_adapter_name(), "tushare")

    def test_radar_engine_imports_scan_contracts_from_provider_router(self) -> None:
        from adapters import provider_router

        radar_scan_engine = _load_module("radar_scan_engine_provider_test", RADAR_ENGINE_PATH)

        self.assertIs(radar_scan_engine.RADAR_PARTIAL_STEPS, provider_router.RADAR_PARTIAL_STEPS)
        self.assertIs(radar_scan_engine.RADAR_ALL_STEPS, provider_router.RADAR_ALL_STEPS)
        self.assertIs(radar_scan_engine.run_named_scan_steps, provider_router.run_named_scan_steps)
        self.assertIs(radar_scan_engine.resolve_radar_trade_date, provider_router.resolve_radar_trade_date)
        self.assertIs(radar_scan_engine.get_all_a_share_stocks, provider_router.get_all_a_share_stocks)

    def test_deep_dive_engine_imports_full_scan_from_provider_router(self) -> None:
        from adapters import provider_router

        deep_sniper_engine = _load_module("deep_sniper_engine_provider_test", DEEP_DIVE_ENGINE_PATH)

        self.assertIs(deep_sniper_engine.run_full_scan, provider_router.run_full_scan)

    def test_get_all_a_share_stocks_marks_fallback_source_meta(self) -> None:
        from adapters import provider_router

        with patch.object(provider_router.SCAN_ADAPTER, "get_all_a_share_stocks", return_value={"status": "error: primary down", "data": []}, create=True):
            with patch.object(provider_router.baostock_adapter, "get_all_a_share_stocks", return_value={"status": "ok", "data": [{"code": "600001"}]}, create=True):
                result = provider_router.get_all_a_share_stocks()

        self.assertEqual(result["_source_meta"]["source_type"], "baostock")
        self.assertEqual(result["_source_meta"]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
