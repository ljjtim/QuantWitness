"""从 LEAN 固定的 Python.NET 源码构建本地关闭补丁。"""
import argparse
from pathlib import Path
import shutil
import subprocess
from urllib.request import urlopen
from zipfile import ZipFile


REVISION = "ba1b2eae84a34ea98301e47732c2a56c59a52821"


def build(home, launcher, download_cache):
    name = f"pythonnet-{REVISION}.zip"
    archive = home / "downloads" / name
    if not archive.exists():
        archive.parent.mkdir(parents=True, exist_ok=True)
        partial = archive.with_suffix(".download")
        if download_cache and (download_cache / name).is_file():
            shutil.copyfile(download_cache / name, partial)
        else:
            url = f"https://codeload.github.com/QuantConnect/pythonnet/zip/{REVISION}"
            with urlopen(url, timeout=60) as response, partial.open("wb") as output:
                shutil.copyfileobj(response, output)
        partial.replace(archive)

    source_dir = home / "rp-integration" / "src"
    source = source_dir / f"pythonnet-{REVISION}"
    with ZipFile(archive) as package:
        package.extractall(source_dir)
    runtime = source / "src/runtime/Runtime.cs"
    code = runtime.read_text(encoding="utf-8-sig")
    # 两次跨运行时回收均无工作时已达到原实现的稳定条件。公开 API 仍按调用者指定的次数执行。
    edits = (
        ("TryCollectingGarbage(runs: 3);", "TryCollectingGarbage(runs: 3, stopWhenStable: true);"),
        ("forceBreakLoops: true", "stopWhenStable: true"),
        ("forceBreakLoops: false", "stopWhenStable: false"),
        ("bool forceBreakLoops)", "bool stopWhenStable)"),
        ("if (attempt + 1 == runs) return true;", """if (stopWhenStable)
                    {
                        Console.WriteLine($"Python.NET shutdown GC: stable after {attempt + 1}/{runs} passes");
                        return true;
                    }
                    if (attempt + 1 == runs) return true;"""),
    )
    for old, new in edits:
        if code.count(old) != 1:
            raise ValueError(f"固定 Python.NET 源码不匹配：{old}")
        code = code.replace(old, new)
    runtime.write_text(code, encoding="utf-8")
    subprocess.run([str(home / "sdk/dotnet.exe"), "build", str(runtime.with_name("Python.Runtime.csproj")),
                    "-c", "Release", "--nologo", "-m:1", "-p:UseSharedCompilation=false"], check=True)
    binary = source / "pythonnet/runtime/Python.Runtime.dll"
    for target in (launcher, launcher / "pythonnet/runtime"):
        shutil.copy2(binary, target / binary.name)
        shutil.copy2(binary.with_suffix(".pdb"), target / "Python.Runtime.pdb")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--launcher", type=Path, required=True)
    parser.add_argument("--download-cache", type=Path)
    options = parser.parse_args()
    build(options.home, options.launcher, options.download_cache)
