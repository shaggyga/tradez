param([string]$Root = "")

$ErrorActionPreference = "Stop"
if (-not $Root) { $Root = Split-Path -Parent $PSScriptRoot }
$Root = [IO.Path]::GetFullPath($Root)
$Trad = [IO.Path]::GetFullPath((Join-Path $Root "trad"))
if ($Trad -ne [IO.Path]::GetFullPath($PSScriptRoot)) {
    throw "Research launcher must target its canonical project."
}
$Supervisor = Join-Path $Trad "oanda_always_on_supervisor.ps1"
$SupervisorPattern = [regex]::Escape($Supervisor)
$existing = @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
    $_.Name -match '^(powershell|pwsh)\.exe$' -and
    $_.CommandLine -match "\s-File\s+`"?$SupervisorPattern`"?(?:\s|$)"
})
if ($existing.Count -gt 0) {
    if ($existing.Count -eq 1 -and $existing[0].CommandLine -match '\s-ResearchCollectionOnly(?:\s|$)') {
        Write-Output "Research collection supervisor is already running."
        exit 0
    }
    throw "A different supervisor is already running; no second supervisor launched."
}
$Logs = Join-Path $Trad "data\oanda_training_manager\logs"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$arguments = @(
    "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
    "-File", "`"$Supervisor`"", "-Root", "`"$Root`"",
    "-AccountKey", "OANDA_ACCOUNT_ID_DUM4",
    "-RunLabel", "research-collection-audit-20260906",
    "-IntervalSec", "30", "-ChildDurationSec", "604800",
    "-SafeCoreOnly", "-ResearchCollectionOnly"
)
$process = Start-Process `
    -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList $arguments -WorkingDirectory $Trad -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $Logs "research_collection_${stamp}.out.log") `
    -RedirectStandardError (Join-Path $Logs "research_collection_${stamp}.err.log") `
    -PassThru
Write-Output ("Research collection supervisor launched, PID " + $process.Id)
