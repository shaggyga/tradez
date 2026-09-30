param([ValidateSet('Status','Validate','Start')][string]$Action='Status')
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$trad=Join-Path $root 'trad'
$profilePath=Join-Path $trad 'config\operational_runtime_current_20260930.json'
$python=Join-Path $env:LOCALAPPDATA 'CodexRuntimes\timeseries312\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Qualified Python runtime unavailable. See START_HERE.md.' }
if ($Action -eq 'Status') {
    & $python -B (Join-Path $trad 'inspect_operational_runtime_v1.py') --profile $profilePath
    exit $LASTEXITCODE
}
# Windows PowerShell preserves profile UTC strings; the existing validator checks expiry.
$command = @'
param($Root,$ProfilePath,$Action)
$ErrorActionPreference='Stop'
$profile=Get-Content -LiteralPath $ProfilePath -Raw | ConvertFrom-Json
$trad=Join-Path $Root 'trad'
& (Join-Path $trad 'start_oanda_operational_research_v6.ps1') -Root $Root -OperationalProfilePath $ProfilePath -RecoveryUntilUtc $profile.recovery_until_utc -ValidateOnly
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Action -eq 'Start') {
    & (Join-Path $trad 'start_oanda_supervisor_watchdog_v6.ps1') -Root $Root -OperationalProfilePath $ProfilePath -RecoveryUntilUtc $profile.recovery_until_utc
}
'@
$escape={param($value) "'"+$value.Replace("'","''")+"'"}
$invoke='& { '+$command+' } -Root '+(& $escape $root)+' -ProfilePath '+(& $escape $profilePath)+' -Action '+(& $escape $Action)
$encoded=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($invoke))
& "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encoded
exit $LASTEXITCODE
