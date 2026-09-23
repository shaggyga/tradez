param([string]$EvidenceRoot = 'C:\Users\zmoor\Documents\forex\practice_recovery_20260910\native_checks')
$ErrorActionPreference='Stop'
$source=Join-Path $PSScriptRoot 'start_oanda_practice_recovery_v1.ps1'
$parseTokens=$null;$parseErrors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($source,[ref]$parseTokens,[ref]$parseErrors)
if ($parseErrors.Count) { throw 'native_parse_errors' }
foreach($function in $ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst]},$false)) {
    . ([scriptblock]::Create($function.Extent.Text))
}
$results=@()
function Assert-Test([bool]$Condition) { if(-not $Condition){throw 'assertion_failed'} }
function Test-Case([string]$Name,[scriptblock]$Body) {
    try { & $Body; $script:results += @{name=$Name;status='passed'} }
    catch { $script:results += @{name=$Name;status='failed';reason='assertion_or_fixture_failed'} }
}
function Expect-Refusal([scriptblock]$Body) { $failed=$false;try{& $Body | Out-Null}catch{$failed=$true};Assert-Test $failed }
function Make-Inventory([int[]]$Super=@(),[int[]]$Conflict=@(),[int[]]$Watch=@(),[int[]]$Work=@()) {
    return @{supervisor_pids=@($Super);conflicting_supervisor_pids=@($Conflict);trial_watchdog_pids=@($Watch);trial_worker_pids=@($Work)}
}
$root='C:\Users\example\forex\trad';$before=1789159400.0;$after=1789159501.0
Test-Case 'native_parser' {Assert-Test ($parseErrors.Count -eq 0)}
Test-Case 'initial_research_first' {Assert-Test ((Get-RecoveryDecision (Make-Inventory) $before $false) -eq 'start_research')}
Test-Case 'one_exact_supervisor_allows_trial' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Super @(1)) $before $false) -eq 'start_trial')}
Test-Case 'existing_watchdog_no_duplicate' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Super @(1) -Watch @(2)) $before $false) -eq 'already_running')}
Test-Case 'orphan_running_worker_adopt_no_duplicate' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Super @(1) -Work @(2,3)) $before $false) -eq 'already_running')}
Test-Case 'conflicting_supervisor_refused' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Conflict @(1)) $before $false) -eq 'conflicting_supervisor_refused')}
Test-Case 'duplicate_expected_supervisors_refused' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Super @(1,2)) $before $false) -eq 'conflicting_supervisor_refused')}
Test-Case 'duplicate_trial_watchdogs_refused' {Assert-Test ((Get-RecoveryDecision (Make-Inventory -Super @(1) -Watch @(2,3)) $before $false) -eq 'duplicate_trial_refused')}
Test-Case 'deadline_no_research_but_close_only_trial' {Assert-Test ((Get-RecoveryDecision (Make-Inventory) $after $false) -eq 'start_trial_close_only')}
Test-Case 'exact_deadline_close_only' {Assert-Test ((Get-RecoveryDecision (Make-Inventory) 1789159500 $false) -eq 'start_trial_close_only')}
Test-Case 'completed_stops_before_deadline' {Assert-Test ((Get-RecoveryDecision (Make-Inventory) $before $true) -eq 'completed_flat')}
Test-Case 'invalid_clock_refuses' {Expect-Refusal {Get-RecoveryDecision (Make-Inventory) ([double]::NaN) $false}}
Test-Case 'normalized_slashes_exact_file_and_scope' {
    $rows=@([pscustomobject]@{Name='powershell.exe';ProcessId=41;CommandLine='powershell.exe -NoProfile -File "C:/Users/example/forex/trad/oanda_always_on_supervisor.ps1" -Root "C:/Users/example/forex" -SafeCoreOnly -ResearchCollectionOnly'})
    $got=Get-RecoveryInventory $root $rows;Assert-Test ($got.supervisor_pids.Count -eq 1 -and $got.supervisor_pids[0] -eq 41)
}
Test-Case 'safe_core_without_research_is_conflict' {
    $got=Get-RecoveryInventory $root @([pscustomobject]@{Name='powershell.exe';ProcessId=42;CommandLine='powershell -File C:\Users\example\forex\trad\oanda_always_on_supervisor.ps1 -Root C:\Users\example\forex -SafeCoreOnly'})
    Assert-Test ($got.conflicting_supervisor_pids.Count -eq 1)
}
Test-Case 'same_basename_different_root_is_conflict' {
    $got=Get-RecoveryInventory $root @([pscustomobject]@{Name='pwsh.exe';ProcessId=43;CommandLine='pwsh -File D:\other\oanda_always_on_supervisor.ps1 -Root D:\other -SafeCoreOnly -ResearchCollectionOnly'})
    Assert-Test ($got.conflicting_supervisor_pids.Count -eq 1)
}
Test-Case 'command_text_does_not_impersonate_file' {
    $got=Get-RecoveryInventory $root @([pscustomobject]@{Name='pwsh.exe';ProcessId=43;CommandLine='pwsh -Command "Write-Output C:\Users\example\forex\trad\oanda_always_on_supervisor.ps1"'})
    Assert-Test ($got.supervisor_pids.Count -eq 0 -and $got.conflicting_supervisor_pids.Count -eq 0)
}
Test-Case 'worker_and_wrapper_detected_with_quotes' {
    $got=Get-RecoveryInventory $root @([pscustomobject]@{Name='python.exe';ProcessId=51;CommandLine='python.exe "C:/Users/example/forex/trad/oanda_practice_trial_runner_v1.py" --run'},[pscustomobject]@{Name='powershell.exe';ProcessId=52;CommandLine='powershell.exe -File "C:/Users/example/forex/trad/start_oanda_practice_trial_v1.ps1"'})
    Assert-Test ($got.trial_worker_pids.Count -eq 1 -and $got.trial_watchdog_pids.Count -eq 1)
}
$fixture=Join-Path $EvidenceRoot ('fixture_'+[guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory((Join-Path $fixture 'config')) | Out-Null
$names=@('start_oanda_practice_recovery_v1.ps1','start_oanda_research_collection.ps1','start_oanda_practice_trial_v1.ps1','oanda_always_on_supervisor.ps1')
$bindings=@{}
foreach($name in $names){$raw=[Text.Encoding]::UTF8.GetBytes('# inert fixture '+$name);[IO.File]::WriteAllBytes((Join-Path $fixture $name),$raw);$bindings[$name]=Get-RecoveryHash $raw}
$config=@{schema_version='practice_trial_runner_v1_20260909';trial_id='practice007_joint_v3_20260909_v1';environment='practice';account_label='practice_007';enabled=$true;user_authorized_practice_trial=$true;stop_epoch=1789159500.0}
$configPath=Join-Path $fixture 'config\practice007_joint_v3_20260909_v1.json'
[IO.File]::WriteAllText($configPath,($config|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
$manifest=@{schema_version='practice_recovery_launcher_v1_20260910';enabled=$true;project_root=$fixture;trial_id='practice007_joint_v3_20260909_v1';stop_epoch=1789159500.0;trial_config_sha256=(Get-RecoveryHash (Read-RecoveryBytes $configPath));source_bindings=$bindings}
$manifestPath=Join-Path $fixture 'config\practice_recovery_launcher_20260910.json'
function Save-FixtureManifest { [IO.File]::WriteAllText($manifestPath,($manifest|ConvertTo-Json -Depth 6),[Text.UTF8Encoding]::new($false)) }
Save-FixtureManifest
Test-Case 'complete_exact_bindings_accept' {$got=Get-RecoveryBindings $fixture $manifestPath;Assert-Test (@($got.source_bindings.PSObject.Properties).Count -eq 4)}
Test-Case 'disabled_manifest_refuses' {$manifest.enabled=$false;Save-FixtureManifest;Expect-Refusal{Get-RecoveryBindings $fixture $manifestPath};$manifest.enabled=$true;Save-FixtureManifest}
Test-Case 'deadline_extension_refuses' {$manifest.stop_epoch=1789159501;Save-FixtureManifest;Expect-Refusal{Get-RecoveryBindings $fixture $manifestPath};$manifest.stop_epoch=1789159500;Save-FixtureManifest}
Test-Case 'wrong_config_raw_hash_refuses' {$prior=$manifest.trial_config_sha256;$manifest.trial_config_sha256=('0'*64);Save-FixtureManifest;Expect-Refusal{Get-RecoveryBindings $fixture $manifestPath};$manifest.trial_config_sha256=$prior;Save-FixtureManifest}
Test-Case 'changed_source_refuses' {$path=Join-Path $fixture $names[1];$prior=[IO.File]::ReadAllBytes($path);[IO.File]::AppendAllText($path,'changed');Expect-Refusal{Get-RecoveryBindings $fixture $manifestPath};[IO.File]::WriteAllBytes($path,$prior)}
Test-Case 'extra_source_inventory_refuses' {$bindings['unexpected.ps1']='0'*64;Save-FixtureManifest;Expect-Refusal{Get-RecoveryBindings $fixture $manifestPath};$bindings.Remove('unexpected.ps1');Save-FixtureManifest}
Test-Case 'bounded_read_refuses' {Expect-Refusal{Read-RecoveryBytes $manifestPath 10}}
Test-Case 'native_reader_sharing_allows_atomic_writer_replace' {
    $readFunction=$ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Read-RecoveryBytes'},$false)[0]
    $open=$readFunction.FindAll({param($node) $node -is [Management.Automation.Language.InvokeMemberExpressionAst] -and $node.Member.Value -eq 'Open'},$true)[0]
    $share=& ([scriptblock]::Create($open.Arguments[3].Extent.Text))
    $old=Join-Path $fixture 'replace_source.json';$moved=Join-Path $fixture 'replace_retained.json'
    [IO.File]::WriteAllText($old,'old')
    $handle=[IO.File]::Open($old,[IO.FileMode]::Open,[IO.FileAccess]::Read,$share)
    try{[IO.File]::Move($old,$moved);[IO.File]::WriteAllText($old,'new');Assert-Test ($handle.ReadByte() -eq 111 -and [IO.File]::ReadAllText($old) -eq 'new')}
    finally{$handle.Dispose()}
}
Test-Case 'new_status_during_read_compared_to_own_completion_clock' {
    $RecoveryRoot=$fixture;$RecoveryState=$fixture;$RecoveryManifest=$manifestPath
    $RecoveryLoadedSourceSha='a'*64;$RecoveryTrial='practice007_joint_v3_20260909_v1';$RecoveryStop=1789159500.0
    $RecoverySchema='practice_recovery_launcher_v1_20260910';$CheckOnly=$true;$Once=$false
    $script:TestWall=100.0
    [IO.File]::WriteAllText((Join-Path $fixture 'status.json'),'{}')
    function Get-RecoveryBindings { return @{source_bindings=@{'start_oanda_practice_recovery_v1.ps1'=('a'*64)}} }
    function Get-RecoveryEpoch { return $script:TestWall }
    function Read-RecoveryBytes {
        $script:TestWall=200.0
        return ,([Text.Encoding]::UTF8.GetBytes('{"schema_version":"practice_trial_runner_v1_20260909","trial_id":"practice007_joint_v3_20260909_v1","environment":"practice","account_label":"practice_007","observed_epoch":150,"stop_epoch":1789159500,"state":"practice_trial_enabled"}'))
    }
    function Get-CimInstance { return @() }
    $got=Get-RecoveryInspection
    Assert-Test ($got.phase -eq 'start_research' -and $got.observed_epoch -eq 200)
}
$statusPath=Join-Path $PSScriptRoot 'data\oanda_training_manager\practice007_joint_v3_20260909_v1\bootstrap.json'
$statusBefore=if(Test-Path -LiteralPath $statusPath){Get-RecoveryHash (Read-RecoveryBytes $statusPath)}else{$null}
foreach($mode in @('-CheckOnly','-Once')) {
    Test-Case ('native_read_only_'+$mode) {
        $out=& "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $source $mode
        $exit=$LASTEXITCODE;$obj=($out -join "`n")|ConvertFrom-Json
        Assert-Test ($exit -in @(0,2) -and $obj.check_only -eq $true -and $obj.broker_action_performed -eq $false)
    }
}
Test-Case 'inspection_did_not_write_bootstrap_status' {$statusAfter=if(Test-Path -LiteralPath $statusPath){Get-RecoveryHash (Read-RecoveryBytes $statusPath)}else{$null};Assert-Test ($statusBefore -ceq $statusAfter)}
$receipt=@{schema_version='practice_recovery_native_checks_v1_20260910';observed_utc=[DateTime]::UtcNow.ToString('o');source_sha256=(Get-RecoveryHash (Read-RecoveryBytes $source));test_sha256=(Get-RecoveryHash (Read-RecoveryBytes $PSCommandPath));passed=@($results|Where-Object status -eq 'passed').Count;failed=@($results|Where-Object status -eq 'failed').Count;cases=$results;no_runtime_launch_or_registration=$true;fixture_path=$fixture}
[IO.Directory]::CreateDirectory($EvidenceRoot)|Out-Null
$receiptPath=Join-Path $EvidenceRoot ('NATIVE_CHECKS_'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfff')+'.json')
[IO.File]::WriteAllText($receiptPath,($receipt|ConvertTo-Json -Depth 6),[Text.UTF8Encoding]::new($false))
$receipt|ConvertTo-Json -Depth 6
if($receipt.failed){exit 1}
