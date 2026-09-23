param(
    [string]$Root = '',
    [Parameter(Mandatory=$true)][string]$OperationalProfilePath,
    [string]$RecoveryUntilUtc = '2026-09-20T23:59:00Z',
    [switch]$ValidateOnly
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'oanda_operational_recovery_contract_v6.ps1')
if (-not $Root) { $Root = Split-Path -Parent $PSScriptRoot }
$Root = Resolve-OperationalRecoveryPath $Root
$Trad = Resolve-OperationalRecoveryPath (Join-Path $Root 'trad')
if ($Trad -ine $PSScriptRoot) { throw 'Operational launcher must target its canonical project.' }
$binding = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
$arguments = @(Get-OperationalResearchSupervisorArguments -Root $Root -ProfilePath $binding.path -RecoveryUntilUtc $RecoveryUntilUtc)
if ($ValidateOnly) {
    @{profile=$binding.path;profile_sha256=$binding.sha256;expires_utc=$binding.expires_utc;
      supervisor_arguments=$arguments;research_only=$true;can_place_orders=$false} | ConvertTo-Json -Depth 4
    exit 0
}
$supervisor = Join-Path $Trad 'oanda_operational_supervisor_v6.ps1'
$escaped = [regex]::Escape($supervisor)
$existing = @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
    $_.Name -match '^(?:powershell|pwsh)\.exe$' -and
    (Test-OperationalOwnerCommand -CommandLine ([string]$_.CommandLine) -Trad $Trad -Role supervisor)
})
if ($existing.Count -gt 0) {
    if ($existing.Count -eq 1 -and (Test-OperationalSupervisorCommand -CommandLine $existing[0].CommandLine -ProfilePath $binding.path)) { exit 0 }
    throw 'Conflicting existing supervisor; operational launcher refuses duplicate or broader recovery.'
}
$env:FOREX_ALLOW_LIVE = '0'
$env:FOREX_LIVE_EXECUTE = '0'
$logs = Join-Path $Trad 'data\oanda_training_manager\logs'
New-Item -ItemType Directory -Force -Path $logs | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss_fff'
$safeArguments = @($arguments | ForEach-Object { ConvertTo-OperationalProcessArgument $_ })
Start-Process -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList $safeArguments -WorkingDirectory $Trad -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $logs "operational_research_launcher_${stamp}.out.log") `
    -RedirectStandardError (Join-Path $logs "operational_research_launcher_${stamp}.err.log") | Out-Null
