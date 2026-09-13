param([string]$OutputName = 'observation.json')
$ErrorActionPreference = 'Stop'
$Trad = 'C:\Users\zmoor\Documents\forex\trad'
$State = Join-Path $Trad 'data\oanda_training_manager\state'
$News = Join-Path $Trad 'data\oanda_training_manager\local_news_sentiment'
$Logs = Join-Path $Trad 'data\oanda_training_manager\logs'
$now = [DateTime]::UtcNow
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.ProcessId -ne $PID -and $_.Name -match '^(pythonw?|powershell|pwsh)\.exe$' -and
    [string]$_.CommandLine -match [regex]::Escape($Trad) -and
    [string]$_.CommandLine -notmatch '(?i)(Get-Content|pytest|review_gate|observe\.ps1)'
} | ForEach-Object {
    $scriptMatch = [regex]::Match([string]$_.CommandLine, '(?i)([a-z0-9_]+\.(py|ps1))(?:"|\s|$)')
    if ($scriptMatch.Success) {
        [pscustomobject]@{pid=$_.ProcessId;parent_pid=$_.ParentProcessId;script=$scriptMatch.Groups[1].Value;research_collection_only=([string]$_.CommandLine -match '\s-ResearchCollectionOnly(?:\s|$)')}
    }
})
$latest = Get-ChildItem -LiteralPath $Logs -Filter 'always_on_supervisor_*.jsonl' -File | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
$events = @(Get-Content -LiteralPath $latest.FullName | ForEach-Object { try { $_ | ConvertFrom-Json } catch {} })
$heartbeat = $events | Where-Object event -eq 'heartbeat' | Select-Object -Last 1
$files = [ordered]@{
    account='account_007_dashboard_v1.json'
    quotes='practice_007_market_quotes_v1.json'
    quote_stream='practice_007_quote_stream_heartbeat_v1.json'
    clock='clock_integrity_v1.json'
    candles='all68_m1_forward_update_heartbeat_v1.json'
    fastlane='source_governance_news_fast_lane_v3.json'
    integrity='project_integrity_audit_v1.json'
    storage='storage_headroom_v1.json'
}
$artifacts = @()
foreach ($entry in $files.GetEnumerator()) {
    $path=Join-Path $State $entry.Value
    if (Test-Path -LiteralPath $path) {
        $f=Get-Item -LiteralPath $path
        $artifacts += [pscustomobject]@{name=$entry.Key;path=$path;bytes=$f.Length;age_sec=[math]::Round(($now-$f.LastWriteTimeUtc).TotalSeconds,2)}
    }
}
$newsHeartbeats = @()
foreach ($name in @('collector_heartbeat_v1.json','official_release_fast_lane_heartbeat_v4.json','official_release_fast_mapping_heartbeat_v3.json')) {
    $path=Join-Path $News $name
    if (Test-Path -LiteralPath $path) {
        $f=Get-Item -LiteralPath $path
        $p=Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        $newsHeartbeats += [pscustomobject]@{name=$name;age_sec=[math]::Round(($now-$f.LastWriteTimeUtc).TotalSeconds,2);phase=$p.phase;classification_version=$p.classification_version}
    }
}
$account=Get-Content -LiteralPath (Join-Path $State $files.account) -Raw | ConvertFrom-Json
$quote=Get-Content -LiteralPath (Join-Path $State $files.quote_stream) -Raw | ConvertFrom-Json
$pageStatus=$null
try { $pageStatus=(Invoke-WebRequest -Uri 'http://127.0.0.1:8765/' -UseBasicParsing -TimeoutSec 10).StatusCode } catch { $pageStatus='unavailable' }
$receipt=[ordered]@{
    observed_utc=$now.ToString('o');processes=$processes
    forex_tasks=@(Get-ScheduledTask | Where-Object TaskName -match '^Forex' | ForEach-Object { [pscustomobject]@{name=$_.TaskName;state=$_.State.ToString()} })
    supervisor_log=$latest.FullName
    supervisor_heartbeat_utc=$heartbeat.time
    managed_running=@($heartbeat.managed | Where-Object running | Select-Object name,pids,freshness)
    managed_blocked_count=@($heartbeat.managed | Where-Object { $_.freshness.reason -eq 'research_collection_only' }).Count
    supervisor_error_count=@($events | Where-Object event -eq 'supervisor_error').Count
    restart_events=@($events | Where-Object { $_.event -match 'stopped|restart' } | Select-Object time,event,name,reason)
    artifacts=$artifacts;news=$newsHeartbeats;dashboard_http_status=$pageStatus
    account=[ordered]@{time=$account.time;environment=$account.environment;aggregate=$account.aggregate}
    quote_stream=[ordered]@{phase=$quote.phase;can_place_orders=$quote.details.can_place_orders;output=$quote.details.output;stream=$quote.details.stream}
}
$output=Join-Path $PSScriptRoot $OutputName
if (Test-Path -LiteralPath $output) { throw 'Observation already exists; choose another name.' }
$receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $output -Encoding utf8
$receipt | ConvertTo-Json -Depth 12
