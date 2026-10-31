param(
    [Parameter(Mandatory=$true)][string]$LeanHome,
    [Parameter(Mandatory=$true)][string]$FromPackage,
    [string]$DownloadCache,
    [string]$NugetCache
)
$ErrorActionPreference='Stop'
$LeanHome=[IO.Path]::GetFullPath($LeanHome)
if(Test-Path -LiteralPath $LeanHome) { throw "干净安装目录必须不存在：$LeanHome" }
$FromPackage=(Resolve-Path -LiteralPath $FromPackage).Path
New-Item -ItemType Directory -Path $LeanHome | Out-Null
foreach($dir in @('downloads','tools','src','sdk','logs','temp','cache/pip','cache/uv')) {
    New-Item -ItemType Directory -Path (Join-Path $LeanHome $dir) -Force | Out-Null
}
$env:TEMP=Join-Path $LeanHome 'temp'; $env:TMP=$env:TEMP
$env:PYTHONDONTWRITEBYTECODE='1'; $env:PYTHONNOUSERSITE='1'
Remove-Item Env:PYTHONHOME,Env:PYTHONPATH -ErrorAction SilentlyContinue
$env:UV_PYTHON_INSTALL_DIR=Join-Path $LeanHome 'python-managed'
$env:UV_PYTHON_BIN_DIR=Join-Path $LeanHome 'tools/python-bin'
$env:UV_CACHE_DIR=Join-Path $LeanHome 'cache/uv'
$env:PIP_CACHE_DIR=Join-Path $LeanHome 'cache/pip'
$revision='a3bf4cc3e1e11a461b44d43894b0c6445f1100e6'
$timer=[Diagnostics.Stopwatch]::StartNew()
$stages=[ordered]@{}
function Fetch($name,$url) {
    $target=Join-Path $LeanHome "downloads/$name"
    if($DownloadCache -and (Test-Path -LiteralPath (Join-Path $DownloadCache $name))) {
        Copy-Item -LiteralPath (Join-Path $DownloadCache $name) -Destination $target
    } else {
        & curl.exe --fail --location --silent --show-error --connect-timeout 30 --output $target $url
        if($LASTEXITCODE -ne 0) { throw "下载失败：$url" }
    }
    return $target
}
function Native($program,$arguments,$logName) {
    & $program @arguments *> (Join-Path $LeanHome "logs/$logName.log")
    if($LASTEXITCODE -ne 0) { Get-Content (Join-Path $LeanHome "logs/$logName.log") -Tail 45; throw "$logName 失败" }
}
$archive=Fetch 'dotnet-sdk-10.0.403-win-x64.zip' 'https://builds.dotnet.microsoft.com/dotnet/Sdk/10.0.403/dotnet-sdk-10.0.403-win-x64.zip'
[IO.Compression.ZipFile]::ExtractToDirectory($archive,(Join-Path $LeanHome 'sdk'))
$archive=Fetch "lean-$revision.zip" "https://github.com/QuantConnect/Lean/archive/$revision.zip"
[IO.Compression.ZipFile]::ExtractToDirectory($archive,(Join-Path $LeanHome 'src'))
$archive=Fetch 'uv-win-x64.zip' 'https://github.com/astral-sh/uv/releases/download/0.13.6/uv-x86_64-pc-windows-msvc.zip'
[IO.Compression.ZipFile]::ExtractToDirectory($archive,(Join-Path $LeanHome 'tools'))
$uv=(Get-ChildItem (Join-Path $LeanHome 'tools') -Recurse -Filter uv.exe | Select-Object -First 1).FullName
Native $uv @('python','install','3.11.18') 'python-install'
$base=(Get-ChildItem (Join-Path $LeanHome 'python-managed') -Directory | Where-Object Name -Like 'cpython-3.11*' | Select-Object -First 1).FullName
Native $uv @('venv','--seed','--python',(Join-Path $base 'python.exe'),(Join-Path $LeanHome 'python-env')) 'venv'
$python=Join-Path $LeanHome 'python-env/Scripts/python.exe'
Native $python @('-B','-m','pip','install','-r',(Join-Path $PSScriptRoot 'requirements.txt'),'build','wheel') 'dependencies'
Native $python @('-B','-X','utf8',(Join-Path $PSScriptRoot 'install_dependencies.py'),'--package',$FromPackage,'--output',(Join-Path $LeanHome 'dependency-evidence')) 'bundle-dependencies'
$project=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$source=Join-Path $LeanHome 'temp/project'
New-Item -ItemType Directory -Path $source | Out-Null
foreach($name in @('pyproject.toml','MANIFEST.in','README.md','ARCHITECTURE.md','CONTRIBUTING.md','EXTENSIONS.md','SECURITY.md','LICENSE','NOTICE','THIRD_PARTY_NOTICES.md','licenses','docs','src')) {
    Copy-Item -LiteralPath (Join-Path $project $name) -Destination $source -Recurse
}
Native $python @('-B','-m','build','--wheel','--outdir',(Join-Path $LeanHome 'wheels'),$source) 'wheel'
$wheel=(Get-ChildItem (Join-Path $LeanHome 'wheels') -Filter '*.whl' | Select-Object -First 1).FullName
Native $python @('-B','-m','pip','install','--no-deps',$wheel) 'install-project'
Native $python @('-B','-m','pip','freeze','--all') 'python-packages'
$stages.dependencies=$timer.Elapsed.TotalSeconds
$leanSource=Join-Path $LeanHome "src/Lean-$revision"
# Python.NET 关闭过程自行管理GIL，避免跨Shutdown保留线程锁。
$initializer=Join-Path $leanSource 'Common/Python/PythonInitializer.cs'
$text=[IO.File]::ReadAllText($initializer)
$needle='                    var pyLock = Py.GIL();'
if(!$text.Contains($needle)) { throw '固定LEAN版本的Python关闭补丁定位失败' }
[IO.File]::WriteAllText($initializer,$text.Replace($needle,'                    // Python.NET关闭时管理GIL。'),[Text.UTF8Encoding]::new($false))
$text=[IO.File]::ReadAllText($initializer)
$needle=@'
                        var code = string.Join(";", toRemove.Select(s => $"sys.path.remove('{s}')"));
'@
if(!$text.Contains($needle)) { throw '固定LEAN版本的Windows虚拟环境路径补丁定位失败' }
$replacement=@'
                        var code = "sys.path[:] = [p for p in sys.path if 'site-packages' not in p.lower()]";
'@
[IO.File]::WriteAllText($initializer,$text.Replace($needle,$replacement),[Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText((Join-Path $LeanHome 'upstream-revision.txt'),$revision,[Text.UTF8Encoding]::new($false))
$env:DOTNET_ROOT=Join-Path $LeanHome 'sdk'; $env:DOTNET_CLI_HOME=Join-Path $LeanHome 'cache/dotnet-home'
$env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE='1'; $env:DOTNET_CLI_TELEMETRY_OPTOUT='1'; $env:DOTNET_NOLOGO='1'
$env:NUGET_PACKAGES=if($NugetCache){[IO.Path]::GetFullPath($NugetCache)}else{Join-Path $LeanHome 'cache/nuget'}
$env:NUGET_HTTP_CACHE_PATH=Join-Path $LeanHome 'cache/http'
$dotnet=Join-Path $LeanHome 'sdk/dotnet.exe'
Native $dotnet @('build',(Join-Path $leanSource 'Launcher/QuantConnect.Lean.Launcher.csproj'),'-c','Release','-m:2','--nologo','-v:minimal') 'launcher-build'
$stages.launcher_build=$timer.Elapsed.TotalSeconds-$stages.dependencies
Native $python @('-B','-X','utf8',(Join-Path $PSScriptRoot 'prepare_metadata.py'),'--package',$FromPackage,'--upstream',$leanSource,'--target',(Join-Path $LeanHome 'input-metadata')) 'metadata'
& (Join-Path $PSScriptRoot 'engine/build.ps1') -LeanHome $LeanHome -MetadataSource (Join-Path $LeanHome 'input-metadata') -NugetCache $NugetCache -Install
if(!$?) { throw 'LEAN插件安装失败' }
$stages.total=$timer.Elapsed.TotalSeconds
$record=[ordered]@{status='pass';lean_home=$LeanHome;python=$python;upstream_revision=$revision;stage_seconds=$stages;project_wheel=$wheel;from_package=$FromPackage;upstream_compatibility_patches=@('pythonnet_shutdown_gil','windows_venv_site_packages_filter')}
$record|ConvertTo-Json -Depth 6|Set-Content (Join-Path $LeanHome 'clean-installation.json') -Encoding utf8
Write-Output "干净安装完成：$LeanHome"

