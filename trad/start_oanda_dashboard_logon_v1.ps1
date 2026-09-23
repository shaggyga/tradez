[CmdletBinding()]
param(
    [string]$ExpectedDashboardSha256 = "",
    [string]$ExpectedBoundarySha256 = "",
    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"
$DashboardPath = Join-Path $PSScriptRoot "oanda_practice_live_dashboard.py"
$BoundaryPath = Join-Path $PSScriptRoot "oanda_dashboard_exclusive_server_v1.py"
$PythonPath = Join-Path $env:LOCALAPPDATA "CodexRuntimes\timeseries312\Scripts\python.exe"

function Test-DashboardProcessMatch {
    param($Process)
    if ($Process.Name -notin @("python.exe", "pythonw.exe")) { return $false }
    $command = ([string]$Process.CommandLine).Replace('/', '\')
    $scriptPattern = [regex]::Escape($DashboardPath.Replace('/', '\'))
    return ($command -match ('(?i)(?:^|\s)"?' + $scriptPattern + '"?(?:\s|$)'))
}

function Assert-DashboardSourcePins {
    foreach ($entry in @(
        @{ Path = $DashboardPath; Expected = $ExpectedDashboardSha256 },
        @{ Path = $BoundaryPath; Expected = $ExpectedBoundarySha256 }
    )) {
        if ($entry.Expected -notmatch '^[0-9a-f]{64}$') { throw "dashboard_logon_source_pin_required" }
        if ((Get-FileHash -LiteralPath $entry.Path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.Expected) {
            throw "dashboard_logon_source_changed"
        }
    }
    if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) { throw "dashboard_logon_python_missing" }
}

# Dot sourcing exposes only the pure match/pin functions for native fixtures.
if ($MyInvocation.InvocationName -eq '.') { return }
Assert-DashboardSourcePins
$mutex = $null
$owned = $false
try {
    if (-not $CheckOnly) {
        $mutex = New-Object System.Threading.Mutex($false, "Local\ForexDashboardLogonV1")
        try { $owned = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $owned = $true }
        if (-not $owned) { Write-Output '{"status":"another_logon_launcher"}'; exit 0 }
    }
    $existing = @(Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" |
        Where-Object { Test-DashboardProcessMatch $_ })
    if ($existing.Count -gt 0) {
        @{ status = "dashboard_process_already_present"; pids = @($existing.ProcessId); inspection_only = [bool]$CheckOnly } |
            ConvertTo-Json -Compress
        exit 0
    }
    if ($CheckOnly) { Write-Output '{"status":"dashboard_absent_would_launch","inspection_only":true}'; exit 0 }
    Assert-DashboardSourcePins
    # python.exe is intentionally used: the unchanged supervisor adopts this
    # exact script argument.  Hidden controls the window, not executable name.
    $child = Start-Process -FilePath $PythonPath -ArgumentList @(
        ('"' + $DashboardPath + '"'), "--host", "127.0.0.1", "--port", "8765",
        "--max-runs", "20", "--max-lines", "20000"
    ) -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
    @{ status = "dashboard_process_launched"; pid = $child.Id; observation_epoch = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 } |
        ConvertTo-Json -Compress
} finally {
    if ($owned) { $mutex.ReleaseMutex() }
    if ($null -ne $mutex) { $mutex.Dispose() }
}
