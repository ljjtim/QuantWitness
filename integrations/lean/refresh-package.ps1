param([Parameter(Mandatory=$true)][string]$LeanHome)
$ErrorActionPreference='Stop'
$LeanHome=(Resolve-Path -LiteralPath $LeanHome).Path
$python=Join-Path $LeanHome 'python-env/Scripts/python.exe'
$project=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$stamp=Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$root=Join-Path $LeanHome "package-builds/$stamp"
$source=Join-Path $root 'source'
New-Item -ItemType Directory -Path $source -Force | Out-Null
$env:TEMP=Join-Path $root 'temp'; $env:TMP=$env:TEMP
New-Item -ItemType Directory -Path $env:TEMP -Force | Out-Null
$env:PYTHONDONTWRITEBYTECODE='1'; $env:PYTHONNOUSERSITE='1'
$env:PIP_CACHE_DIR=Join-Path $LeanHome 'cache/pip'
Remove-Item Env:PYTHONHOME,Env:PYTHONPATH -ErrorAction SilentlyContinue
foreach($name in @('pyproject.toml','MANIFEST.in','README.md','ARCHITECTURE.md','CONTRIBUTING.md','EXTENSIONS.md','SECURITY.md','LICENSE','NOTICE','THIRD_PARTY_NOTICES.md','licenses','docs','src')) {
 Copy-Item -LiteralPath (Join-Path $project $name) -Destination $source -Recurse
}
& $python -B -m build --wheel --outdir (Join-Path $root 'wheels') $source *> (Join-Path $root 'build.log')
if($LASTEXITCODE -ne 0) { throw "wheel构建失败：$root/build.log" }
$wheel=(Get-ChildItem (Join-Path $root 'wheels') -Filter '*.whl'|Select-Object -First 1).FullName
& $python -B -m pip install --no-deps --force-reinstall $wheel *> (Join-Path $root 'install.log')
if($LASTEXITCODE -ne 0) { throw "wheel安装失败：$root/install.log" }
[ordered]@{status='pass';wheel=$wheel;python=$python;source_snapshot=$source;installed_at=(Get-Date -Format o)}|ConvertTo-Json|Set-Content (Join-Path $LeanHome 'package-installation.json') -Encoding utf8
Write-Output "源码wheel已更新；安装锁须重新转换准入：$wheel"
