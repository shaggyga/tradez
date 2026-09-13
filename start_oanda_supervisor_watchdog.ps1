param([string]$Root = "")

$ErrorActionPreference = "Stop"
if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}
$Watchdog = Join-Path $Root "trad\oanda_supervisor_watchdog.ps1"
$Logs = Join-Path $Root "trad\data\oanda_training_manager\logs"
$escaped = [regex]::Escape($Watchdog)

if (-not (Test-Path -LiteralPath $Watchdog)) {
    throw "Supervisor watchdog is missing: $Watchdog"
}
$existing = @(
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -match '^(?:powershell|pwsh)\.exe$' -and
            [string]$_.CommandLine -match "\s-File\s+`"?$escaped`"?(?:\s|$)"
        }
)
if ($existing.Count -gt 0) { exit 0 }

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
Start-Process `
    -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList $arguments `
    -WorkingDirectory (Join-Path $Root "trad") `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr | Out-Null
