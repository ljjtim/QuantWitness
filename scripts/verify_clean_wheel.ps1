param(
    [string]$ProjectRoot = "",
    [string]$Python = "python",
    [Parameter(Mandatory = $true)]
    [string]$Wheel,
    [Parameter(Mandatory = $true)]
    [string]$ReceiptOut,
    [Parameter(Mandatory = $true)]
    [string]$CatalogLock
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
if (-not $ProjectRoot) {
    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
}

function Get-BytesSha256([byte[]]$Bytes) {
    $hasher = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($hasher.ComputeHash($Bytes))).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $hasher.Dispose()
    }
}

function Get-TextSha256([string]$Text) {
    return Get-BytesSha256 ([Text.Encoding]::UTF8.GetBytes($Text))
}

$commands = [Collections.Generic.List[object]]::new()
function Invoke-AcceptanceCommand(
    [string]$Id,
    [string]$Executable,
    [string[]]$Arguments,
    [bool]$ExpectSuccess = $true
) {
    $start = Get-Date
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $Executable
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    $info.CreateNoWindow = $true
    foreach ($argument in $Arguments) {
        [void]$info.ArgumentList.Add($argument)
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    $exitCode = $process.ExitCode
    $expectationMet = ($ExpectSuccess -and $exitCode -eq 0) -or (-not $ExpectSuccess -and $exitCode -ne 0)
    $argumentText = $Arguments -join "`0"
    $commands.Add([ordered]@{
        command_id = $Id
        argument_hash = Get-TextSha256 $argumentText
        expected = if ($ExpectSuccess) { "success" } else { "failure" }
        exit_code = $exitCode
        expectation_met = $expectationMet
        stdout_sha256 = Get-TextSha256 $stdout
        stderr_sha256 = Get-TextSha256 $stderr
        wall_milliseconds = [Math]::Max(1, [int]((Get-Date) - $start).TotalMilliseconds)
    })
    if (-not $expectationMet) {
        throw "干净 wheel 命令未满足预期: $Id (exit=$exitCode)"
    }
    return [pscustomobject]@{ Stdout = $stdout; Stderr = $stderr; ExitCode = $exitCode }
}

$project = [IO.Path]::GetFullPath($ProjectRoot)
$wheelPath = [IO.Path]::GetFullPath($Wheel)
$receiptPath = [IO.Path]::GetFullPath($ReceiptOut)
$repositoryRoot = [IO.Path]::GetFullPath((Split-Path -Parent $project))
$repositoryPrefix = $repositoryRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar
) + [IO.Path]::DirectorySeparatorChar
if (-not (Test-Path -LiteralPath $wheelPath -PathType Leaf)) {
    throw "显式指定的 wheel 不存在"
}
if (
    $receiptPath.Equals($repositoryRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $receiptPath.StartsWith($repositoryPrefix, [StringComparison]::OrdinalIgnoreCase)
) {
    throw "receipt 输出必须位于仓库外"
}
if (Test-Path -LiteralPath $receiptPath) {
    throw "receipt 输出必须不存在"
}
$catalogLockPath = [IO.Path]::GetFullPath($CatalogLock)
if (-not (Test-Path -LiteralPath (Join-Path $catalogLockPath "CURRENT") -PathType Leaf)) {
    throw "显式 Catalog Lock 缺少 CURRENT"
}
$lockPath = [IO.Path]::GetFullPath((Join-Path $project "release\dependency-distributions.json"))
$inventoryVerifier = [IO.Path]::GetFullPath((Join-Path $project "tools\wheel_source_inventory.py"))
$temporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = [IO.Path]::GetFullPath((Join-Path $temporaryRoot ("research-pipeline-wheel-" + [guid]::NewGuid().ToString("N"))))
if (-not $work.StartsWith($temporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "临时目录越界"
}

New-Item -ItemType Directory -Path $work | Out-Null
$oldPythonPath = $env:PYTHONPATH
try {
    $inventoryResult = Invoke-AcceptanceCommand "wheel.inventory" $Python @(
        $inventoryVerifier, "--project", $project, "--wheel", $wheelPath
    )
    $inventory = $inventoryResult.Stdout | ConvertFrom-Json

    $env:PYTHONPATH = (Join-Path $project "src")
    $sourceCapabilityResult = Invoke-AcceptanceCommand "source.capabilities" $Python @(
        "-m", "research_pipeline", "capabilities", "--format", "json"
    )
    $sourceCapabilities = $sourceCapabilityResult.Stdout.TrimEnd()

    $venvPath = Join-Path $work "venv"
    [void](Invoke-AcceptanceCommand "venv.create" $Python @(
        "-m", "venv", "--system-site-packages", $venvPath
    ))
    $venvPython = Join-Path $venvPath "Scripts\python.exe"
    [void](Invoke-AcceptanceCommand "wheel.install" $venvPython @(
        "-m", "pip", "install", "--no-index", "--no-deps", "--ignore-installed", $wheelPath
    ))

    $env:PYTHONPATH = ""
    Push-Location $work
    try {
        $originProbe = (
            "import pathlib,research_pipeline,sys;" +
            "root=pathlib.Path(sys.argv[1]).resolve();" +
            "origin=pathlib.Path(research_pipeline.__file__).resolve();" +
            "assert root not in origin.parents,origin;print('isolated-wheel')"
        )
        [void](Invoke-AcceptanceCommand "installed.import-origin" $venvPython @(
            "-c", $originProbe, $project
        ))
        $versionResult = Invoke-AcceptanceCommand "installed.version" $venvPython @(
            "-m", "research_pipeline", "--version"
        )
        [void](Invoke-AcceptanceCommand "installed.help" $venvPython @(
            "-m", "research_pipeline", "--help"
        ))
        $installedCapabilityResult = Invoke-AcceptanceCommand "installed.capabilities" $venvPython @(
            "-m", "research_pipeline", "capabilities", "--format", "json"
        )
        $installedCapabilities = $installedCapabilityResult.Stdout.TrimEnd()
        if ($installedCapabilities -cne $sourceCapabilities) {
            throw "源码与 wheel capabilities 输出不一致"
        }
        $resourceProbe = (
            "from importlib.resources import files;" +
            "from research_pipeline.catalog import CompiledCatalog;" +
            "from research_pipeline.domain import load_session_policy_bundle;" +
            "import sys;" +
            "r=files('research_pipeline');" +
            "required=['capabilities.json','gate_ia_protocol.json','packages/templates/package.yaml'," +
            "'domain/rule_snapshots/minute_reference_rules.json'];" +
            "assert all(r.joinpath(p).is_file() for p in required);" +
            "assert not r.joinpath('catalog/default_lock/CURRENT').is_file();" +
            "print(CompiledCatalog.load(sys.argv[1]).catalog_hash,load_session_policy_bundle().bundle_hash)"
        )
        $resourceResult = Invoke-AcceptanceCommand "installed.package-data" $venvPython @(
            "-c", $resourceProbe, $catalogLockPath
        )
        [void](Invoke-AcceptanceCommand "installed.dependencies" $venvPython @(
            "-c", "from research_pipeline.platform import verify_dependency_distribution_lock;import sys;print(len(verify_dependency_distribution_lock(sys.argv[1])))", $lockPath
        ))

        $package = Join-Path $work "package"
        [void](Invoke-AcceptanceCommand "package.init" $venvPython @(
            "-m", "research_pipeline", "package", "init", $package, "--json"
        ))
        [void](Invoke-AcceptanceCommand "package.lint" $venvPython @(
            "-m", "research_pipeline", "package", "lint", "--package", $package, "--json"
        ))

        $recipeListResult = Invoke-AcceptanceCommand "recipe.list" $venvPython @(
            "-m", "research_pipeline", "recipe", "list", "--format", "json"
        )
        $recipeList = $recipeListResult.Stdout | ConvertFrom-Json
        if ($recipeList.data.items.Count -ne 0) {
            throw "公共 recipe 当前应为空"
        }
        [void](Invoke-AcceptanceCommand "recipe.unknown-id" $venvPython @(
            "-m", "research_pipeline", "recipe", "describe", "unknown.recipe", "--format", "json"
        ) $false)
    }
    finally {
        Pop-Location
    }

    $environmentResult = Invoke-AcceptanceCommand "installed.environment" $venvPython @(
        "-c", "import json,platform,sys;print(json.dumps({'python':platform.python_version(),'cache_tag':sys.implementation.cache_tag,'platform':platform.system().lower()},sort_keys=True))"
    )
    $environment = $environmentResult.Stdout | ConvertFrom-Json
    $payload = [ordered]@{
        contract_version = "research-clean-wheel-acceptance-v1"
        status = "pass"
        wheel_name = [IO.Path]::GetFileName($wheelPath)
        wheel_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $wheelPath).Hash.ToLowerInvariant()
        dependency_lock_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $lockPath).Hash.ToLowerInvariant()
        environment = $environment
        installed_version = $versionResult.Stdout.Trim()
        inventory = $inventory
        capabilities_sha256 = Get-TextSha256 $installedCapabilities
        package_data_probe_sha256 = Get-TextSha256 $resourceResult.Stdout
        recipe_list_sha256 = Get-TextSha256 $recipeListResult.Stdout
        validated_capability_ids = @("capability.discovery", "research_package.plan")
        commands = $commands
        source_tree_on_pythonpath_during_installed_checks = $false
        database_access = "none"
        network_access = "none"
    }
    $payloadJson = $payload | ConvertTo-Json -Depth 20 -Compress
    $payloadPath = Join-Path $work "receipt-payload.json"
    $payloadJson | Set-Content -LiteralPath $payloadPath -Encoding utf8NoBOM
    $canonicalHash = & $Python -c "import hashlib,json,sys;p=json.load(open(sys.argv[1],encoding='utf-8'));b=json.dumps(p,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode();print(hashlib.sha256(b).hexdigest())" $payloadPath
    if ($LASTEXITCODE -ne 0 -or $canonicalHash -notmatch '^[0-9a-f]{64}$') {
        throw "receipt canonical hash 计算失败"
    }
    $receipt = [ordered]@{
        payload = $payload
        receipt_payload_sha256 = $canonicalHash
    }
    $parent = Split-Path -Parent $receiptPath
    if (-not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    $temporaryReceipt = $receiptPath + ".tmp"
    if (Test-Path -LiteralPath $temporaryReceipt) {
        throw "receipt 临时输出已存在"
    }
    $receipt | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $temporaryReceipt -Encoding utf8NoBOM
    Move-Item -LiteralPath $temporaryReceipt -Destination $receiptPath
}
finally {
    $env:PYTHONPATH = $oldPythonPath
    $resolved = [IO.Path]::GetFullPath($work)
    if ($resolved.StartsWith($temporaryRoot, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath $resolved)) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Output "clean wheel verification passed: $receiptPath"
