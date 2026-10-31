param(
    [Parameter(Mandatory = $true)][string]$LeanHome,
    [string]$MetadataSource,
    [string]$NugetCache,
    [switch]$Install
)

$ErrorActionPreference = 'Stop'
$LeanHome = (Resolve-Path -LiteralPath $LeanHome).Path
$buildRoot = Join-Path $LeanHome 'rp-integration'
$revision = (Get-Content -LiteralPath (Join-Path $LeanHome 'upstream-revision.txt') -Raw).Trim()
$leanSource = Join-Path $LeanHome "src/Lean-$revision"
$launcher = Join-Path $leanSource 'Launcher/bin/Release'
$dotnet = Join-Path $LeanHome 'sdk/dotnet.exe'
$python = Join-Path $LeanHome 'python-env/Scripts/python.exe'
foreach ($file in @($dotnet, $python, (Join-Path $launcher 'QuantConnect.Lean.Launcher.dll'))) {
    if (!(Test-Path -LiteralPath $file -PathType Leaf)) { throw "缺少已安装的 LEAN 依赖：$file" }
}
foreach ($folder in @('temp', 'logs', 'cache/dotnet-home', 'cache/nuget', 'cache/http', 'cache/nuget-plugins', 'assets')) {
    New-Item -ItemType Directory -Path (Join-Path $buildRoot $folder) -Force | Out-Null
}
$env:DOTNET_ROOT = Join-Path $LeanHome 'sdk'
$env:DOTNET_CLI_HOME = Join-Path $buildRoot 'cache/dotnet-home'
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE = '1'
$env:DOTNET_NOLOGO = '1'
$env:NUGET_PACKAGES = if ($NugetCache) { [IO.Path]::GetFullPath($NugetCache) } else { Join-Path $buildRoot 'cache/nuget' }
$env:NUGET_HTTP_CACHE_PATH = Join-Path $buildRoot 'cache/http'
$env:NUGET_PLUGINS_CACHE_PATH = Join-Path $buildRoot 'cache/nuget-plugins'
$env:TEMP = Join-Path $buildRoot 'temp'
$env:TMP = $env:TEMP
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PATH = "$env:DOTNET_ROOT;$env:PATH"

foreach ($project in @('QuantWitnessLeanHost.csproj', 'parquet-direct/ParquetInput.csproj')) {
    $log = Join-Path $buildRoot "logs/$([IO.Path]::GetFileNameWithoutExtension($project))-build.log"
    & $dotnet build (Join-Path $PSScriptRoot $project) -c Release --nologo "-p:LeanBin=$launcher" "-p:BuildRoot=$buildRoot" "-p:RestorePackagesPath=$env:NUGET_PACKAGES" *> $log
    if ($LASTEXITCODE -ne 0) { Get-Content -LiteralPath $log -Tail 60; throw "编译失败：$project" }
    Get-Content -LiteralPath $log -Tail 5
}
$hostBin = Join-Path $buildRoot 'bin/QuantWitnessLeanHost/net10.0'
$parquetBin = Join-Path $buildRoot 'bin/ParquetInput/net10.0'
$assets = Join-Path $buildRoot 'assets'
$dlls = @('QuantWitnessLeanHost.dll', 'ChinaRules.dll', 'CorporateActions.dll', 'ParquetInput.dll',
    'ParquetSharp.dll', 'Apache.Arrow.dll', 'ParquetSharpNative.dll')
foreach ($name in $dlls) {
    $source = if ($name -in @('QuantWitnessLeanHost.dll', 'ChinaRules.dll', 'CorporateActions.dll')) {
        Join-Path $hostBin $name
    } elseif ($name -eq 'ParquetSharpNative.dll') {
        Join-Path $parquetBin 'runtimes/win-x64/native/ParquetSharpNative.dll'
    } else { Join-Path $parquetBin $name }
    Copy-Item -LiteralPath $source -Destination (Join-Path $assets $name) -Force
}
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'ResearchLeanAlgorithm.py') -Destination $buildRoot -Force
Copy-Item -LiteralPath (Join-Path $leanSource 'Launcher/config.json') -Destination (Join-Path $buildRoot 'config-template.json') -Force
if (!$MetadataSource) { $MetadataSource = Join-Path $LeanHome 'input-metadata' }
& $python -X utf8 -B (Join-Path $PSScriptRoot 'install_metadata.py') --source $MetadataSource --target (Join-Path $buildRoot 'metadata')
if ($LASTEXITCODE -ne 0) { throw '市场元数据安装失败' }
if ($Install) {
    foreach ($name in $dlls) { Copy-Item -LiteralPath (Join-Path $assets $name) -Destination (Join-Path $launcher $name) -Force }
}
$manifest = [ordered]@{
    lean_home = $LeanHome
    lean_source = $leanSource
    launcher = $launcher
    dotnet = $dotnet
    python = $python
    python_venv = (Join-Path $LeanHome 'python-env')
    algorithm = (Join-Path $buildRoot 'ResearchLeanAlgorithm.py')
    algorithm_type_name = 'ResearchLeanAlgorithm'
    algorithm_relative = 'ResearchLeanAlgorithm.py'
    config_template = (Join-Path $buildRoot 'config-template.json')
    metadata = (Join-Path $buildRoot 'metadata')
    host_assembly = 'QuantWitnessLeanHost'
    data_feed = 'QuantWitness.LeanInput.ParquetDataFeed'
    assets = $assets
    installed = [bool]$Install
    files = @($dlls) + @('ResearchLeanAlgorithm.py')
}
$manifestPath = Join-Path $buildRoot 'installation.json'
[IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 5), [Text.UTF8Encoding]::new($false))
Write-Output "构建完成，安装清单：$manifestPath"
