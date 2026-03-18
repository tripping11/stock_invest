import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SHARED_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_DIR))

from adapters import provider_router
from engines import public_backtest_dataset_engine


class Tier1CacheCompatibilityTests(unittest.TestCase):
    def test_load_scan_cache_prefers_canonical_tier1_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            canonical = base / "tier1_scan.json"
            legacy = base / "akshare_scan.json"
            canonical.write_text(json.dumps({"source": "canonical"}, ensure_ascii=False), encoding="utf-8")
            legacy.write_text(json.dumps({"source": "legacy"}, ensure_ascii=False), encoding="utf-8")

            loaded = provider_router.load_scan_cache(base)

        self.assertEqual(loaded["source"], "canonical")

    def test_load_scan_cache_falls_back_to_legacy_akshare_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            legacy = base / "akshare_scan.json"
            legacy.write_text(json.dumps({"source": "legacy"}, ensure_ascii=False), encoding="utf-8")

            loaded = provider_router.load_scan_cache(base)

        self.assertEqual(loaded["source"], "legacy")

    def test_run_full_scan_writes_canonical_and_legacy_cache_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            out_dir = Path(temp_dir) / "600328"
            with patch.object(
                provider_router,
                "FULL_SCAN_STEPS",
                [("company_profile", lambda _code: {"data": {"股票简称": "中盐化工"}, "evidence": {"field_name": "company_profile"}, "status": "ok"})],
            ), patch.object(
                provider_router.akshare_adapter,
                "_resolve_scan_step",
                side_effect=lambda stock_code, step_name, fetcher, cached_results=None: fetcher(stock_code),
            ):
                result = provider_router.run_full_scan("600328", str(out_dir))

            self.assertEqual(result["_scan_provider"], provider_router.get_scan_adapter_name())
            self.assertTrue((out_dir / "tier1_scan.json").exists())
            self.assertTrue((out_dir / "tier1_evidence.json").exists())
            self.assertTrue((out_dir / "akshare_scan.json").exists())
            self.assertTrue((out_dir / "akshare_evidence.json").exists())

    def test_public_backtest_cache_loader_reads_canonical_tier1_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            raw_dir = repo_root / "data" / "raw" / "600328"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "tier1_scan.json").write_text(
                json.dumps({"company_profile": {"data": {"股票简称": "中盐化工"}}}, ensure_ascii=False),
                encoding="utf-8",
            )

            loaded = public_backtest_dataset_engine._load_local_cached_scan("600328", repo_root=repo_root)

        self.assertEqual(loaded["company_profile"]["data"]["股票简称"], "中盐化工")


if __name__ == "__main__":
    unittest.main()
