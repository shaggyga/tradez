$ErrorActionPreference = "Continue"

$Root = "D:\forex\trad"
$Python = "C:\Users\zmoor\AppData\Roaming\uv\python\cpython-3.12-windows-x86_64-none\python.exe"
$SitePackages = Join-Path $Root "..venv\Lib\site-packages"
$RuntimeLogs = Join-Path $Root "data\runtime_logs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$WatchLog = Join-Path $RuntimeLogs "live_account_watchdog_${Stamp}.log"
$SummaryJson = Join-Path $RuntimeLogs "live_account_watchdog_latest.json"
$DurationHours = 8
$ConfiguredEndAt = $env:LIVE_WATCHDOG_END_AT
$IntervalSeconds = 300
$StaleMinutes = 20

New-Item -ItemType Directory -Force -Path $RuntimeLogs | Out-Null

function Write-WatchLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $WatchLog -Value $line
}

function Get-LaneProcess {
    param([string]$Script)
    Get-CimInstance Win32_Process |
        Where-Object { $_.Name -match '^python(\.exe)?$' -and $_.CommandLine -like "*$Script*" } |
        Select-Object -First 1
}

function Get-StateAgeMinutes {
    param([string]$RelativePath)
    $path = Join-Path $Root $RelativePath
    if (-not (Test-Path -LiteralPath $path)) {
        return $null
    }
    $item = Get-Item -LiteralPath $path
    return [math]::Round(((Get-Date) - $item.LastWriteTime).TotalMinutes, 2)
}

function Start-Lane {
    param(
        [string]$Name,
        [string]$Script,
        [string[]]$ExtraArgs
    )
    $scriptPath = Join-Path $Root $Script
    $out = Join-Path $RuntimeLogs ("watchdog_restart_{0}_{1}.out.log" -f $Name, (Get-Date -Format "yyyyMMdd_HHmmss"))
    $err = Join-Path $RuntimeLogs ("watchdog_restart_{0}_{1}.err.log" -f $Name, (Get-Date -Format "yyyyMMdd_HHmmss"))
    $bootstrap = "import runpy,sys; root=r'$Root'; site=r'$SitePackages'; script=sys.argv[1]; sys.path.insert(0,root); sys.path.append(site); sys.argv=[script]+sys.argv[2:]; runpy.run_path(script,run_name='__main__')"
    $escapedBootstrap = $bootstrap -replace '"','\"'
    $args = '-S -c "' + $escapedBootstrap + '" "' + $scriptPath + '"'
    foreach ($arg in $ExtraArgs) {
        $args += ' "' + $arg + '"'
    }
    $env:TRAD_PROJECT_ROOT = $Root
    $env:PYTHONUTF8 = "1"
    $proc = Start-Process -FilePath $Python -ArgumentList $args -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    Write-WatchLog ("restart_started lane={0} pid={1} out={2} err={3}" -f $Name, $proc.Id, $out, $err)
}

$lanes = @(
    [pscustomobject]@{
        Name = "gpt_live"
        Script = "oanda_gpt_prod_live_account_manager.py"
        State = "data\forex_gpt_manager\account_gpt_prod_live\state.json"
        ExtraArgs = @("--no-scan-on-launch")
        StaleMinutes = 90
    },
    [pscustomobject]@{
        Name = "primary_live"
        Script = "oanda_primary_challenger_live_account_manager.py"
        State = "data\technical_scout_manager\account_live_primary_challenger_scout\state.json"
        ExtraArgs = @("--no-scan-on-launch")
        StaleMinutes = 20
    },
    [pscustomobject]@{
        Name = "tech_live"
        Script = "oanda_tech_prod_live_account_manager.py"
        State = "data\technical_scout_manager\account_live_tech_broad_regime_scout\state.json"
        ExtraArgs = @("--no-scan-on-launch")
        StaleMinutes = 20
    }
)

$endAt = if ($ConfiguredEndAt) {
    [datetime]::Parse($ConfiguredEndAt)
} else {
    (Get-Date).AddHours($DurationHours)
}
Write-WatchLog ("watchdog_started duration_hours={0} interval_seconds={1} stale_minutes={2}" -f $DurationHours, $IntervalSeconds, $StaleMinutes)

while ((Get-Date) -lt $endAt) {
    $snapshot = [ordered]@{
        time = (Get-Date).ToString("o")
        endAt = $endAt.ToString("o")
        lanes = @()
    }

    foreach ($lane in $lanes) {
        $proc = Get-LaneProcess -Script $lane.Script
        $age = Get-StateAgeMinutes -RelativePath $lane.State
        $laneStaleMinutes = if ($lane.StaleMinutes) { [double]$lane.StaleMinutes } else { [double]$StaleMinutes }
        $status = "ok"
        $reason = ""

        if ($null -eq $proc) {
            $status = "missing"
            $reason = "process not found"
            Start-Lane -Name $lane.Name -Script $lane.Script -ExtraArgs $lane.ExtraArgs
        } elseif ($null -eq $age) {
            $status = "missing_state"
            $reason = "state file missing"
        } elseif ($age -gt $laneStaleMinutes) {
            $status = "stale"
            $reason = "state age ${age}m > ${laneStaleMinutes}m"
            Write-WatchLog ("stale lane={0} pid={1} age_minutes={2} stale_limit_minutes={3}" -f $lane.Name, $proc.ProcessId, $age, $laneStaleMinutes)
        }

        if ($status -eq "ok") {
            Write-WatchLog ("ok lane={0} pid={1} state_age_minutes={2}" -f $lane.Name, $proc.ProcessId, $age)
        } else {
            Write-WatchLog ("issue lane={0} status={1} reason={2}" -f $lane.Name, $status, $reason)
        }

        $snapshot.lanes += [ordered]@{
            name = $lane.Name
            script = $lane.Script
            pid = if ($proc) { $proc.ProcessId } else { $null }
            stateAgeMinutes = $age
            staleLimitMinutes = $laneStaleMinutes
            status = $status
            reason = $reason
        }
    }

    $snapshot | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $SummaryJson -Encoding UTF8
    Start-Sleep -Seconds $IntervalSeconds
}

Write-WatchLog "watchdog_finished"
