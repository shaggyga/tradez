$ErrorActionPreference='Stop'
$projectRoot='C:\Users\zmoor\Documents\forex\trad'
$evidenceRoot='C:\Users\zmoor\Documents\forex\overnight_curve_buildout_20260909'
if([IO.Path]::GetFullPath($PSScriptRoot) -ne $evidenceRoot){throw 'Unexpected observation workspace'}
$output=Join-Path $evidenceRoot 'process_usage_002'
if(Test-Path -LiteralPath $output){throw 'Observation output already exists'}
$null=New-Item -ItemType Directory -Path $output
function Read-Usage {
    $started=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0
    $rows=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^(python|pythonw|pwsh|powershell)(\.exe)?$'} | ForEach-Object {
        $match=[regex]::Match([string]$_.CommandLine,'(?i)(?:"([^"]+\.(?:py|ps1))"|([^\s"]+\.(?:py|ps1)))(?:\s|$)')
        $path=if($match.Groups[1].Success){$match.Groups[1].Value}else{$match.Groups[2].Value}
        if($path.StartsWith($projectRoot+'\',[StringComparison]::OrdinalIgnoreCase)){
            [pscustomobject]@{pid=[int]$_.ProcessId;parent_pid=[int]$_.ParentProcessId;created_utc=$_.CreationDate.ToUniversalTime().ToString('o');script_path=$path;
                kernel_cpu_100ns=[decimal]$_.KernelModeTime;user_cpu_100ns=[decimal]$_.UserModeTime;working_set_bytes=[decimal]$_.WorkingSetSize;
                private_page_bytes=[decimal]$_.PrivatePageCount;thread_count=[int]$_.ThreadCount;handle_count=[int]$_.HandleCount;
                read_transfer_bytes=[decimal]$_.ReadTransferCount;write_transfer_bytes=[decimal]$_.WriteTransferCount}
        }
    })
    $os=Get-CimInstance Win32_OperatingSystem
    $computer=Get-CimInstance Win32_ComputerSystem
    [ordered]@{read_started_epoch=$started;read_completed_epoch=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0;
        logical_processor_count=[int]$computer.NumberOfLogicalProcessors;total_visible_memory_kib=[decimal]$os.TotalVisibleMemorySize;
        free_physical_memory_kib=[decimal]$os.FreePhysicalMemory;processes=$rows}
}
$before=Read-Usage
$before | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $output 'BEFORE.json') -Encoding utf8
Start-Sleep -Seconds 45
$after=Read-Usage
$after | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $output 'AFTER.json') -Encoding utf8
$elapsed=($after.read_started_epoch+$after.read_completed_epoch-$before.read_started_epoch-$before.read_completed_epoch)/2.0
$measurements=@($after.processes | ForEach-Object {
    $new=$_
    $old=@($before.processes | Where-Object {$_.pid -eq $new.pid -and $_.created_utc -eq $new.created_utc -and $_.script_path -eq $new.script_path})
    if($old.Count -eq 1){
        $cpu=($new.kernel_cpu_100ns+$new.user_cpu_100ns-$old[0].kernel_cpu_100ns-$old[0].user_cpu_100ns)/10000000.0
        if($cpu -lt 0){throw 'Process CPU counter regressed'}
        [ordered]@{pid=$new.pid;script_path=$new.script_path;cpu_seconds=$cpu;approximate_cpu_one_core_percent=100.0*$cpu/$elapsed;
            approximate_cpu_all_logical_cores_percent=100.0*$cpu/$elapsed/$after.logical_processor_count;
            latest_working_set_bytes=$new.working_set_bytes;latest_private_page_bytes=$new.private_page_bytes;
            latest_thread_count=$new.thread_count;latest_handle_count=$new.handle_count;
            read_transfer_bytes_delta=$new.read_transfer_bytes-$old[0].read_transfer_bytes;
            write_transfer_bytes_delta=$new.write_transfer_bytes-$old[0].write_transfer_bytes}
    }
})
$result=[ordered]@{schema_version='project_process_resource_observation_v2_20260909';observed_utc=(Get-Date).ToUniversalTime().ToString('o');
    approximate_wall_interval_sec=$elapsed;logical_processor_count=$after.logical_processor_count;before_process_count=$before.processes.Count;
    after_process_count=$after.processes.Count;matched_process_count=$measurements.Count;
    before_free_physical_memory_kib=$before.free_physical_memory_kib;after_free_physical_memory_kib=$after.free_physical_memory_kib;
    total_visible_memory_kib=$after.total_visible_memory_kib;measurements=$measurements;
    limitations=@('Bounded live observation, not a controlled benchmark. Audit jobs outside the project script path are excluded.',
        'CIM rows are read sequentially; CPU rates use snapshot-midpoint elapsed time and are approximate.',
        'Working sets may share pages and must not be summed as unique physical RAM. Transfer counters are process I/O, not a measured physical-disk throughput.',
        'No process priorities, worker state, model sources or system settings are changed.');
    helper_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant();runtime_writes=$false}
$result | ConvertTo-Json -Depth 7 | Set-Content -LiteralPath (Join-Path $output 'PROCESS_RESOURCE_OBSERVATION_20260909.json') -Encoding utf8
[ordered]@{path=(Join-Path $output 'PROCESS_RESOURCE_OBSERVATION_20260909.json');matched_processes=$measurements.Count;elapsed_sec=$elapsed} | ConvertTo-Json -Compress
