import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "notebooks" / "00_common"))
sys.path.append(str(ROOT / "scripts"))

from global_config import load_global_config
from validate_configs import validate_csv_headers


class TestGlobalConfig(unittest.TestCase):
    def test_env_default_substitution(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "global_config.yaml"
            p.write_text("organization:\n  environment: ${MISSING_ENV:dev}\n", encoding="utf-8")
            cfg = load_global_config(str(p))
            self.assertEqual(cfg["organization"]["environment"], "dev")


class TestConfigValidation(unittest.TestCase):
    def test_validation_passes_for_repo_configs(self):
        config_dir = ROOT / "config"
        errors = validate_csv_headers(config_dir)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
