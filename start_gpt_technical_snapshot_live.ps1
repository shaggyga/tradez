param(
    [switch]$Execute,
    [switch]$ScanNow,
    [switch]$Once,
    [switch]$StatusOnly,
    [switch]$AllowSharedAccount,
    [switch]$WaitForReady,
    [int]$DurationHours = 10,
    [int]$IntervalSeconds = 120,
    [int]$MaxChecks = 0
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSCommandPath
$RuntimeLogs = Join-Path $Root "data\runtime_logs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$SummaryPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_start_latest.json"
$LogPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_start_$Stamp.log"
$JsonlPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_handoff_$Stamp.jsonl"
$StdoutPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_$Stamp.stdout.log"
$StderrPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_$Stamp.stderr.log"

New-Item -ItemType Directory -Force -Path $RuntimeLogs | Out-Null

function Write-StartLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $LogPath -Value $line
}

function Write-Summary {
    param([hashtable]$Payload)
    $Payload["time"] = (Get-Date).ToString("o")
    $Payload["logPath"] = $LogPath
    $Payload | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $SummaryPath -Encoding UTF8
}

function Write-HandoffEvent {
    param([hashtable]$Payload)
    $Payload["time"] = (Get-Date).ToString("o")
    ($Payload | ConvertTo-Json -Depth 12 -Compress) | Add-Content -LiteralPath $JsonlPath
}

function Quote-ProcessArgument {
    param([string]$Value)
    if ($null -eq $Value) {
        return '""'
    }
    return '"' + ($Value -replace '"', '\"') + '"'
}

$PythonCandidates = @(
    (Join-Path $env:APPDATA "uv\python\cpython-3.12-windows-x86_64-none\python.exe"),
    (Join-Path $Root "..\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe"),
    (Join-Path $Root "data\oanda_training_manager\.research_py313\Scripts\python.exe"),
    (Join-Path $Root "..venv\Scripts\python.exe"),
    "python"
)

$Python = $PythonCandidates | Where-Object {
    try { Get-Command $_ -ErrorAction Stop | Out-Null; $true } catch { $false }
} | Select-Object -First 1

if (-not $Python) {
    Write-Summary @{
        status = "error"
        reason = "No Python executable found."
        started = $false
    }
    throw "No Python executable found for GPT technical snapshot live starter."
}

$StatusScript = Join-Path $Root "oanda_gpt_technical_snapshot_live_status.py"
$ManagerScript = Join-Path $Root "oanda_gpt_technical_snapshot_account_manager.py"
$Bootstrap = "import runpy,sys; root=r'$Root'; site=r'$Root\..venv\Lib\site-packages'; script=sys.argv[1]; sys.path.insert(0,root); sys.path.append(site); sys.argv=[script]+sys.argv[2:]; runpy.run_path(script,run_name='__main__')"

if ($Execute) {
    if (-not $env:FOREX_ALLOW_LIVE) {
        $env:FOREX_ALLOW_LIVE = "True"
    }
    if (-not $env:FOREX_LIVE_CONFIRM) {
        $env:FOREX_LIVE_CONFIRM = "GPT_TECH_SNAPSHOT_LIVE_REAL_MONEY"
    }
}

function Get-SnapshotStatus {
    $StatusErrPath = Join-Path $RuntimeLogs "gpt_technical_snapshot_live_status_$Stamp.stderr.log"
    $text = & $Python -S -c $Bootstrap $StatusScript 2> $StatusErrPath
    $exitCode = 0
    $lastExit = Get-Variable -Name LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue
    if ($null -ne $lastExit) {
        $exitCode = [int]$lastExit.Value
    }
    if ($exitCode -ne 0) {
        Write-Summary @{
            status = "error"
            reason = "status script failed"
            exitCode = $exitCode
            statusErrorPath = $StatusErrPath
            started = $false
        }
        throw "Status script failed with exit code $exitCode."
    }
    $jsonText = $text | Out-String
    if ([string]::IsNullOrWhiteSpace($jsonText)) {
        Write-Summary @{
            status = "error"
            reason = "status script produced no JSON"
            statusErrorPath = $StatusErrPath
            started = $false
        }
        throw "Status script produced no JSON."
    }
    return [pscustomobject]@{
        Text = $jsonText
        Object = ($jsonText | ConvertFrom-Json)
    }
}

$statusResult = Get-SnapshotStatus
$statusText = $statusResult.Text
$status = $statusResult.Object
$tech = $status.technical_snapshot_live

if ($StatusOnly) {
    Write-StartLog "status_only"
    Write-Summary @{
        status = "status_only"
        started = $false
        technicalSnapshotLive = $tech
        sharedLiveAccount = $status.shared_live_account
    }
    $statusText
    exit 0
}

if ($WaitForReady -and (-not $AllowSharedAccount)) {
    $deadline = (Get-Date).AddHours($DurationHours)
    $checks = 0
    while (-not $tech.activation_ready) {
        $checks += 1
        $reason = ($tech.activation_blockers -join "; ")
        $nextCheck = (Get-Date).AddSeconds($IntervalSeconds)
        Write-StartLog ("waiting check={0} reason={1} next_check={2}" -f $checks, $reason, $nextCheck.ToString("o"))
        $waitingPayload = @{
            status = "waiting"
            started = $false
            check = $checks
            reason = $reason
            nextSafeAction = $tech.next_safe_action
            nextCheck = $nextCheck.ToString("o")
            deadline = $deadline.ToString("o")
            technicalSnapshotLive = $tech
            sharedLiveAccount = $status.shared_live_account
            handoffLogPath = $JsonlPath
        }
        Write-Summary $waitingPayload
        Write-HandoffEvent $waitingPayload

        if (($MaxChecks -gt 0 -and $checks -ge $MaxChecks) -or (Get-Date) -ge $deadline) {
            $timeoutPayload = @{
                status = "timed_out"
                started = $false
                checks = $checks
                reason = $reason
                nextSafeAction = $tech.next_safe_action
                deadline = $deadline.ToString("o")
                technicalSnapshotLive = $tech
                sharedLiveAccount = $status.shared_live_account
                handoffLogPath = $JsonlPath
            }
            Write-StartLog ("timed_out checks={0} reason={1}" -f $checks, $reason)
            Write-Summary $timeoutPayload
            Write-HandoffEvent $timeoutPayload
            exit 2
        }

        Start-Sleep -Seconds $IntervalSeconds
        $statusResult = Get-SnapshotStatus
        $statusText = $statusResult.Text
        $status = $statusResult.Object
        $tech = $status.technical_snapshot_live
    }
    Write-StartLog "activation_ready"
}

if ((-not $tech.activation_ready) -and (-not $AllowSharedAccount)) {
    $reason = ($tech.activation_blockers -join "; ")
    Write-StartLog ("refused reason={0}" -f $reason)
    Write-Summary @{
        status = "refused"
        reason = $reason
        nextSafeAction = $tech.next_safe_action
        started = $false
        technicalSnapshotLive = $tech
        sharedLiveAccount = $status.shared_live_account
    }
    exit 2
}

$args = @("-S", "-c", $Bootstrap, $ManagerScript)
if ($ScanNow) {
    $args += "--scan-now"
}
if ($Once) {
    $args += "--once"
}
if (-not $ScanNow) {
    $args += "--no-scan-on-launch"
}
if ($Execute) {
    $args += "--execute"
    $args += "--i-understand-live-risk"
} else {
    $args += "--dry-run"
}

if ($AllowSharedAccount) {
    $env:FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT = "True"
}
$env:PYTHONUTF8 = "1"
$CleanPath = [Environment]::GetEnvironmentVariable("Path", "Process")
if (-not $CleanPath) {
    $CleanPath = [Environment]::GetEnvironmentVariable("PATH", "Process")
}
if ($CleanPath) {
    [Environment]::SetEnvironmentVariable("PATH", $null, "Process")
    [Environment]::SetEnvironmentVariable("Path", $CleanPath, "Process")
}

$ArgumentString = (($args | ForEach-Object { Quote-ProcessArgument ([string]$_) }) -join " ")
$proc = Start-Process -FilePath $Python -ArgumentList $ArgumentString -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $StdoutPath -RedirectStandardError $StderrPath -PassThru
Write-StartLog ("started pid={0} execute={1} scan_now={2} once={3} stdout={4} stderr={5}" -f $proc.Id, [bool]$Execute, [bool]$ScanNow, [bool]$Once, $StdoutPath, $StderrPath)
Write-Summary @{
    status = "started"
    started = $true
    pid = $proc.Id
    execute = [bool]$Execute
    scanNow = [bool]$ScanNow
    once = [bool]$Once
    allowSharedAccount = [bool]$AllowSharedAccount
    stdoutPath = $StdoutPath
    stderrPath = $StderrPath
    technicalSnapshotLive = $tech
}
