param(
    [string]$Root = "",
    [int]$IntervalSec = 30,
    [int]$MaximumOutputAgeSec = 300,
    [int]$StartupGraceSec = 900,
    [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"

if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}
$Trad = Join-Path $Root "trad"
$Collector = Join-Path $Trad "oanda_local_news_sentiment.py"
$Output = Join-Path $Trad "data\oanda_training_manager\local_news_sentiment\collector_latest_v1.json"
$Logs = Join-Path $Trad "data\oanda_training_manager\logs"
$ProjectPython = Join-Path $Trad "data\oanda_training_manager\.research_py313\Scripts\python.exe"
$CorePython = Join-Path $env:LOCALAPPDATA "CodexRuntimes\timeseries312\Scripts\python.exe"
$Python = if ($PythonPath) {
    $PythonPath
} elseif (Test-Path -LiteralPath $ProjectPython) {
    $ProjectPython
} elseif (Test-Path -LiteralPath $CorePython) {
    $CorePython
} else {
    throw "No project or core Python runtime is available."
}

New-Item -ItemType Directory -Force -Path $Logs | Out-Null
$env:FOREX_ALLOW_LIVE = "0"
$env:FOREX_LIVE_EXECUTE = "0"
$SupervisorLog = Join-Path $Logs (
    "local_news_supervisor_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".jsonl"
)
$Mutex = [System.Threading.Mutex]::new(
    $false,
    "Global\ForexOandaLocalNewsSupervisorV1"
)
$MutexAcquired = $false

function Write-Event {
    param([string]$Event, [hashtable]$Fields = @{})
    $payload = [ordered]@{
        time_utc = (Get-Date).ToUniversalTime().ToString("o")
        event = $Event
    }
    foreach ($key in $Fields.Keys) {
        $payload[$key] = $Fields[$key]
    }
    $payload | ConvertTo-Json -Compress -Depth 5 |
        Add-Content -LiteralPath $SupervisorLog -Encoding utf8
}

function Get-Collectors {
    @(Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -eq "python.exe" -and
            $_.CommandLine -like ("*" + $Collector + "*")
        })
}

function Get-OutputAgeSec {
    if (-not (Test-Path -LiteralPath $Output)) {
        return [double]::PositiveInfinity
    }
    return (
        (Get-Date).ToUniversalTime() -
        (Get-Item -LiteralPath $Output).LastWriteTimeUtc
    ).TotalSeconds
}

function Start-Collector {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdout = Join-Path $Logs ("local_news_sentiment_supervised_" + $stamp + ".out.log")
    $stderr = Join-Path $Logs ("local_news_sentiment_supervised_" + $stamp + ".err.log")
    $arguments = @(
        $Collector,
        "--interval-sec", "60",
        "--duration-sec", "0"
    )
    $process = Start-Process -FilePath $Python `
        -ArgumentList $arguments `
        -WorkingDirectory $Trad `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    Write-Event "collector_started" @{
        pid = $process.Id
        stdout = $stdout
        stderr = $stderr
        python = $Python
    }
    return $process
}

try {
    try {
        $MutexAcquired = $Mutex.WaitOne(0)
    } catch [System.Threading.AbandonedMutexException] {
        $MutexAcquired = $true
    }
    if (-not $MutexAcquired) {
        Write-Output "Another local-news supervisor owns the global mutex."
        exit 3
    }

    Write-Event "supervisor_started" @{
        root = $Root
        interval_sec = $IntervalSec
        maximum_output_age_sec = $MaximumOutputAgeSec
        startup_grace_sec = $StartupGraceSec
    }
    while ($true) {
        $collectors = Get-Collectors
        $outputAgeSec = Get-OutputAgeSec
        if ($collectors.Count -eq 0) {
            $process = Start-Collector
        } elseif ($outputAgeSec -gt $MaximumOutputAgeSec) {
            $youngestCollector = @(
                $collectors |
                    Where-Object { $null -ne $_.CreationDate } |
                    Sort-Object CreationDate -Descending |
                    Select-Object -First 1
            )
            $youngestAgeSec = if ($youngestCollector.Count -gt 0) {
                (
                    (Get-Date).ToUniversalTime() -
                    $youngestCollector[0].CreationDate.ToUniversalTime()
                ).TotalSeconds
            } else {
                [double]::PositiveInfinity
            }
            if ($youngestAgeSec -gt $StartupGraceSec) {
                foreach ($collectorProcess in $collectors) {
                    Stop-Process -Id $collectorProcess.ProcessId -Force -ErrorAction SilentlyContinue
                }
                Write-Event "collector_restarted" @{
                    prior_pids = @($collectors | ForEach-Object { $_.ProcessId })
                    output_age_sec = [math]::Round($outputAgeSec, 1)
                }
                Start-Sleep -Seconds 2
                $process = Start-Collector
            }
        }
        Start-Sleep -Seconds ([math]::Max(5, $IntervalSec))
    }
} finally {
    if ($MutexAcquired) {
        $Mutex.ReleaseMutex()
    }
    $Mutex.Dispose()
}
