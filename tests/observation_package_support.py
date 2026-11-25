"""测试共用的完整日频观察研究包。"""

from pathlib import Path
import shutil


OBSERVATION_PACKAGE = Path(__file__).resolve().parent / "fixtures" / "daily_observation"


def copy_observation_package(destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    return Path(shutil.copytree(OBSERVATION_PACKAGE, destination))
