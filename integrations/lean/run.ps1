param(
 [Parameter(Mandatory=$true)][string]$FromPackage,
 [Parameter(Mandatory=$true)][string]$CatalogLock,
 [string]$DataDb,
 [string]$InputSnapshotManifest,
 [string]$MinuteRoot,
 [Parameter(Mandatory=$true)][string]$Output,
 [string]$LeanHome,
 [string]$DownloadCache,
 [string]$NugetCache,
 [switch]$RefreshPackage,
 [switch]$PersistInputs
)
$ErrorActionPreference='Stop'
if((!!$DataDb) -eq (!!$InputSnapshotManifest)) {
 throw '必须且只能提供 DataDb 或 InputSnapshotManifest'
}
$Output=[IO.Path]::GetFullPath($Output)
if(Test-Path -LiteralPath $Output) { throw '输出目录必须不存在' }
if(!$LeanHome) { $LeanHome=$Output+'-installation' }
if(!(Test-Path -LiteralPath $LeanHome)) {
 & (Join-Path $PSScriptRoot 'install.ps1') -LeanHome $LeanHome -FromPackage $FromPackage -DownloadCache $DownloadCache -NugetCache $NugetCache
}
if($RefreshPackage) { & (Join-Path $PSScriptRoot 'refresh-package.ps1') -LeanHome $LeanHome }
$python=Join-Path $LeanHome 'python-env/Scripts/python.exe'
$env:PYTHONDONTWRITEBYTECODE='1'
Remove-Item Env:PYTHONPATH,Env:PYTHONHOME -ErrorAction SilentlyContinue
$runArguments=@('--from-package',$FromPackage,'--lean-home',$LeanHome,'--catalog-lock',$CatalogLock,'--output',$Output)
if($DataDb) { $runArguments+=@('--data-db',$DataDb) }
else { $runArguments+=@('--input-snapshot-manifest',$InputSnapshotManifest) }
if($MinuteRoot) { $runArguments+=@('--minute-root',$MinuteRoot) }
if($PersistInputs) { $runArguments+='--persist-inputs' }
& $python -B -X utf8 (Join-Path $PSScriptRoot 'accept.py') @runArguments
if($LASTEXITCODE -ne 0) { throw "正式验收失败，见 $Output/acceptance-timing.json" }
