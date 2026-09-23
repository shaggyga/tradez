param(
    [string]$Root = ""
)

$ErrorActionPreference = "Stop"

if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}

$Supervisor = Join-Path $Root "trad\oanda_always_on_supervisor.ps1"
$Logs = Join-Path $Root "trad\data\oanda_training_manager\logs"
$RootPattern = [regex]::Escape($Root)
$SupervisorPattern = [regex]::Escape($Supervisor)

if (-not (Test-Path -LiteralPath $Supervisor)) {
    throw "Safe-core supervisor is missing: $Supervisor"
}

$existing = @(
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -ieq "powershell.exe" -and
            $_.CommandLine -notmatch '\s-Command\s' -and
            $_.CommandLine -match "\s-File\s+`"?$SupervisorPattern`"?(?:\s|$)" -and
            $_.CommandLine -match $RootPattern
        }
)
if ($existing.Count -gt 0) {
    exit 0
}

New-Item -ItemType Directory -Force -Path $Logs | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$stdout = Join-Path $Logs "safe_core_launcher_${stamp}.out.log"
$stderr = Join-Path $Logs "safe_core_launcher_${stamp}.err.log"
$arguments = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", $Supervisor,
    "-Root", $Root,
    "-AccountKey", "OANDA_ACCOUNT_ID_DUM4",
    "-RunLabel", "unified-signal-confidence-matrix-v10",
    "-IntervalSec", "30",
    "-ChildDurationSec", "604800",
    "-SafeCoreOnly"
)

Start-Process -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList $arguments `
    -WorkingDirectory (Join-Path $Root "trad") `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr | Out-Null
