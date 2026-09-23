param(
    [string]$Root = "",
    [string]$OperationalProfilePath = "",
    [string]$RecoveryUntilUtc = "2026-09-20T23:59:00Z"
)

$ErrorActionPreference = 'Stop'
if (-not $OperationalProfilePath) { throw 'V6 watchdog launcher requires explicit operational profile.' }
. (Join-Path $PSScriptRoot 'oanda_operational_recovery_contract_v6.ps1')
if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}
$Watchdog = Join-Path $Root "trad\oanda_supervisor_watchdog_v6.ps1"
$Logs = Join-Path $Root "trad\data\oanda_training_manager\logs"
$escaped = [regex]::Escape($Watchdog)
if ($OperationalProfilePath) {
    $Trad = Resolve-OperationalRecoveryPath (Join-Path $Root 'trad')
    if ($Trad -ine $PSScriptRoot) { throw 'Watchdog launcher must target canonical project.' }
    $binding = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
    $OperationalProfilePath = $binding.path
}

if (-not (Test-Path -LiteralPath $Watchdog)) {
    throw "Supervisor watchdog is missing: $Watchdog"
}
$existing = @(
    Get-CimInstance Win32_Process -ErrorAction Stop |
        Where-Object {
            $_.Name -match '^(?:powershell|pwsh)\.exe$' -and
            (Test-OperationalOwnerCommand -CommandLine ([string]$_.CommandLine) -Trad $Trad -Role watchdog)
        }
)
if ($existing.Count -gt 0) {
    if ($OperationalProfilePath) {
        $expected = [regex]::Escape($OperationalProfilePath)
        $expiry = [regex]::Escape($RecoveryUntilUtc)
        if ($existing.Count -ne 1 -or
            [string]$existing[0].CommandLine -notmatch ('(?i)\s-File\s+"?'+[regex]::Escape($Watchdog)+'"?(?:\s|$)') -or
            [string]$existing[0].CommandLine -notmatch "\s-OperationalProfilePath\s+`"?$expected`"?(?:\s|$)" -or
            [string]$existing[0].CommandLine -notmatch "\s-RecoveryUntilUtc\s+`"?$expiry`"?(?:\s|$)") {
            throw 'Existing watchdog has a different or legacy profile; explicit operational reconciliation required.'
        }
    }
    exit 0
}

New-Item -ItemType Directory -Force -Path $Logs | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$stdout = Join-Path $Logs "supervisor_watchdog_${stamp}.out.log"
$stderr = Join-Path $Logs "supervisor_watchdog_${stamp}.err.log"
$arguments = @(
    "-NoLogo", "-NoProfile", "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", $Watchdog,
    "-Root", $Root
)
if ($OperationalProfilePath) {
    $arguments += @('-OperationalProfilePath',$OperationalProfilePath,'-RecoveryUntilUtc',$RecoveryUntilUtc)
    $arguments = @($arguments | ForEach-Object { ConvertTo-OperationalProcessArgument $_ })
}
Start-Process `
    -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList $arguments `
    -WorkingDirectory (Join-Path $Root "trad") `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr | Out-Null
