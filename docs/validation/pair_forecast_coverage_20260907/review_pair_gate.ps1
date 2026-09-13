$ErrorActionPreference = "Stop"
$project = "C:\Users\zmoor\Documents\forex\trad"
$supervisorPath = Join-Path $project "oanda_always_on_supervisor.ps1"
$launcherPath = Join-Path $project "start_oanda_research_collection.ps1"
$supervisorSourceHash = (Get-FileHash -LiteralPath $supervisorPath -Algorithm SHA256).Hash.ToLowerInvariant()
$launcherSourceHash = (Get-FileHash -LiteralPath $launcherPath -Algorithm SHA256).Hash.ToLowerInvariant()
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($supervisorPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw "Supervisor parse errors" }
$launcher = [System.Management.Automation.Language.Parser]::ParseFile($launcherPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw "Launcher parse errors" }
$assignments = @($ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.AssignmentStatementAst] }, $true))
$allowAssignment = @($assignments | Where-Object { $_.Left.Extent.Text -eq '$ResearchCollectionNames' })
if ($allowAssignment.Count -ne 1) { throw "Allow-list assignment count" }
. ([scriptblock]::Create($allowAssignment[0].Extent.Text))
$expected = @('account_snapshot','live_dashboard','local_news_sentiment','official_release_fast_lane','official_release_fast_mapper','source_governance_news_fast_lane','practice_007_quote_stream','clock_integrity_monitor','all68_m1_forward_archive','project_integrity_audit','storage_headroom_guard','causal_forecast_study_v1','eurusd_local_forecast_study','pair_local_forecast_study_v1')
if (Compare-Object ($expected | Sort-Object) ($ResearchCollectionNames | Sort-Object)) { throw "Unexpected allow-list" }
$definition = @($ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Start-ManagedProcess' }, $true))
if ($definition.Count -ne 1) { throw "Managed function count" }
. ([scriptblock]::Create($definition[0].Extent.Text))
$script:stopped = @()
function Get-MatchingPython { param([string]$Needle) return [pscustomobject]@{ ProcessId=999999; CreationDate=(Get-Date) } }
function Stop-MatchingPython { param($Name,$Needle,$Processes,$Reason) $script:stopped += $Name }
function Start-Process { throw "REAL PROCESS START IS FORBIDDEN IN THIS TEST" }
function Stop-Process { throw "REAL PROCESS STOP IS FORBIDDEN IN THIS TEST" }
function Test-FreshOutput { throw "Blocked workers must not reach freshness checks" }
$ResearchCollectionOnly = $true
$SafeCoreOnly = $true
$EnableCrypto = $true
$EnableModelGapLive = $true
$EnableSubMinuteResearch = $true
$EnableLegacyFlatPracticeBots = $true
$DisabledNames = @()
$SafeCoreSkippedNames = @()
$calls = @($ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Start-ManagedProcess' }, $true))
$managedNames = @($calls | ForEach-Object { $_.CommandElements[2].Value } | Sort-Object -Unique)
$blockedCount = 0
foreach ($name in @($managedNames + 'unknown_future_worker')) {
    $result = Start-ManagedProcess -Name $name -Needle "$name.py" -Arguments @('unused.py')
    if ($name -in $expected) {
        if (-not $result.running -or $result.started) { throw "Allowed worker behavior: $name" }
    } else {
        if ($result.running -or $result.started -or $result.freshness.reason -ne 'research_collection_only') { throw "Exclusion failed: $name" }
        if ($name -notin $script:stopped) { throw "Existing blocked worker not stopped through stub: $name" }
        $blockedCount++
    }
}
# Existing hard-disable policies still take precedence for admitted names.
$DisabledNames = @('account_snapshot')
$result = Start-ManagedProcess -Name 'account_snapshot' -Needle 'unused.py' -Arguments @('unused.py')
if ($result.freshness.reason -ne 'disabled') { throw 'Hard-disable bypass' }
$DisabledNames = @()
$SafeCoreSkippedNames = @('live_dashboard')
$result = Start-ManagedProcess -Name 'live_dashboard' -Needle 'unused.py' -Arguments @('unused.py')
if ($result.freshness.reason -ne 'safe_core_only') { throw 'Safe-core bypass' }
$SafeCoreSkippedNames = @()
$ResearchCollectionOnly = $false
$result = Start-ManagedProcess -Name 'practice_007_fast_executor' -Needle 'unused.py' -Arguments @('unused.py')
if (-not $result.running) { throw 'Normal-mode behavior unexpectedly changed' }
$quoteAssignment = @($assignments | Where-Object { $_.Left.Extent.Text -eq '$DedicatedQuoteSnapshot' })
if ($quoteAssignment.Count -ne 1) { throw 'Quote assignment count' }
$State = 'C:\ReviewState'
. ([scriptblock]::Create($quoteAssignment[0].Extent.Text))
if ($DedicatedQuoteSnapshot -ne 'C:\ReviewState\practice_007_market_quotes_transport_check_v1.json') { throw 'Normal quote destination' }
$ResearchCollectionOnly = $true
. ([scriptblock]::Create($quoteAssignment[0].Extent.Text))
if ($DedicatedQuoteSnapshot -ne 'C:\ReviewState\practice_007_market_quotes_v1.json') { throw 'Research quote destination' }
$starts = @($ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Start-Process' }, $true))
if ($starts.Count -ne 1) { throw 'Unexpected extra process launch' }
$ancestor = $starts[0].Parent
while ($null -ne $ancestor -and $ancestor -isnot [System.Management.Automation.Language.FunctionDefinitionAst]) { $ancestor = $ancestor.Parent }
if ($null -eq $ancestor -or $ancestor.Name -ne 'Start-ManagedProcess') { throw 'Start bypasses gate' }
$quoteCalls = @($calls | Where-Object { $_.CommandElements[2].Value -eq 'practice_007_quote_stream' })
if ($quoteCalls.Count -ne 1 -or $quoteCalls[0].Extent.Text -notmatch '\$DedicatedQuoteSnapshot') { throw 'Quote destination not wired' }
if ($launcher.Extent.Text -notmatch '"-SafeCoreOnly", "-ResearchCollectionOnly"') { throw 'Launcher omits mode' }
if ($launcher.Extent.Text -notmatch '-WindowStyle Hidden') { throw 'Launcher visible' }
# Evaluate the new worker call with a recording stub, never a process API.
$pairCalls = @($calls | Where-Object { $_.CommandElements[2].Value -eq 'pair_local_forecast_study_v1' })
if ($pairCalls.Count -ne 1) { throw 'Exactly one new pair worker is required' }
$Trad='C:\IsolatedPairGate\trad'
$DataRoot=Join-Path $Trad 'data\oanda_training_manager'
$CoreTimeseriesPython='C:\IsolatedPairGate\timeseries312\Scripts\python.exe'
$PairStudyConfig=Join-Path $Trad 'config\pair_local_forecast_study_v1_20260907.json'
$ChildDurationSec=604800
function Start-ManagedProcess {
    param($Name,$Needle,$Arguments,$Freshness,$Executable,$PriorityClass)
    $script:pairCall = @{ Name=$Name; Needle=$Needle; Arguments=$Arguments; Freshness=$Freshness; Executable=$Executable; PriorityClass=$PriorityClass }
}
. ([scriptblock]::Create($pairCalls[0].Extent.Text))
if ($script:pairCall.Name -ne 'pair_local_forecast_study_v1' -or $script:pairCall.Needle -ne 'oanda_pair_local_forecast_study_v1.py') { throw 'Pair worker identity' }
if ($script:pairCall.Executable -ne $CoreTimeseriesPython -or $script:pairCall.PriorityClass -ne 'BelowNormal') { throw 'Pair runtime or priority' }
$expectedArguments=@((Join-Path $Trad 'oanda_pair_local_forecast_study_v1.py'),'--config',$PairStudyConfig,'--duration-sec','604800')
if (Compare-Object $expectedArguments $script:pairCall.Arguments) { throw 'Pair command arguments' }
$expectedFreshness=@{LiteralPath=(Join-Path $DataRoot 'pair_local_forecast_study_v1\heartbeat.json');MaxAgeSec=90;StartupGraceSec=180;ExpectedJsonField='schema_version';ExpectedJsonValue='pair_local_forecast_heartbeat_v1_20260907'}
if ($script:pairCall.Freshness.Count -ne $expectedFreshness.Count) { throw 'Pair heartbeat field count' }
foreach($key in $expectedFreshness.Keys) { if($script:pairCall.Freshness[$key] -ne $expectedFreshness[$key]) { throw "Pair heartbeat field: $key" } }
if ($pairCalls[0].Extent.Text -match 'activate|execute|promot') { throw 'Supervision cannot activate or execute orders' }

# Exercise only the new inert registry gate against in-memory JSON fixtures.
$pairAssignment = @($assignments | Where-Object { $_.Left.Extent.Text -eq '$PairStudyConfig' })
$mutexAssignment = @($assignments | Where-Object { $_.Left.Extent.Text -eq '$SupervisorMutex' })
if($pairAssignment.Count -ne 1 -or $mutexAssignment.Count -ne 1){throw 'Pair config gate boundaries'}
$scriptText=$ast.Extent.Text
$pairBlock=$scriptText.Substring($pairAssignment[0].Extent.StartOffset,$mutexAssignment[0].Extent.StartOffset-$pairAssignment[0].Extent.StartOffset)
function Test-Path { param($LiteralPath) return $script:pairFixtureExists }
function Get-Content { param($LiteralPath,[switch]$Raw) return $script:pairFixtureText }
$validConfig=@{schema_version='pair_local_forecast_registry_v1_20260907';collection_enabled=$true;research_only=$true;can_place_orders=$false;can_promote=$false;can_authorize=$false;account_eligible=$false;proof_eligible=$false;historical_rows_imported=$false}
$configCases=@(
    @{name='valid';changes=@{};expected=$true},
    @{name='disabled';changes=@{collection_enabled=$false};expected=$false},
    @{name='string_enabled';changes=@{collection_enabled='true'};expected=$false},
    @{name='wrong_schema';changes=@{schema_version='unknown'};expected=$false},
    @{name='orders';changes=@{can_place_orders=$true};expected=$false},
    @{name='string_false';changes=@{can_place_orders='false'};expected=$false},
    @{name='promotion';changes=@{can_promote=$true};expected=$false},
    @{name='authorization';changes=@{can_authorize=$true};expected=$false},
    @{name='account';changes=@{account_eligible=$true};expected=$false},
    @{name='proof';changes=@{proof_eligible=$true};expected=$false},
    @{name='imports';changes=@{historical_rows_imported=$true};expected=$false},
    @{name='not_research';changes=@{research_only=$false};expected=$false},
    @{name='missing_flag';changes=@{can_place_orders=$null};expected=$false}
)
foreach($case in $configCases){
    $config=@{}+$validConfig
    foreach($key in $case.changes.Keys){$config[$key]=$case.changes[$key]}
    $script:pairFixtureExists=$true;$script:pairFixtureText=$config|ConvertTo-Json -Compress
    $DisabledNames=@()
    . ([scriptblock]::Create($pairBlock))
    if($PairStudyEnabled -ne $case.expected){throw ('Registry case failed: '+$case.name)}
    if(($DisabledNames -contains 'pair_local_forecast_study_v1') -eq $case.expected){throw ('Registry disable mismatch: '+$case.name)}
}
foreach($case in @('missing','malformed')){
    $script:pairFixtureExists=($case -ne 'missing');$script:pairFixtureText='{invalid'
    $DisabledNames=@()
    . ([scriptblock]::Create($pairBlock))
    if($PairStudyEnabled -or $DisabledNames -notcontains 'pair_local_forecast_study_v1'){throw ('Registry fail-open: '+$case)}
}

[ordered]@{
    status='passed'; allowed_worker_count=$expected.Count;
    pair_worker_fixed_command_and_heartbeat=$true; pair_registry_gate_cases=($configCases.Count+2);
    source_and_activation_validation_delegated_to_order_incapable_worker=$true;
    blocked_worker_names_tested=$blockedCount; managed_name_count=$managedNames.Count;
    unknown_future_worker_blocked=$true; existing_disallowed_workers_stopped_using_stubs=$true;
    normal_quote_path_preserved=$true; research_canonical_quote_path_verified=$true;
    only_launch_site_inside_gate=$true; process_starts=0; process_stops=0; runtime_writes=0;
    supervisor_sha256=$supervisorSourceHash;
    launcher_sha256=$launcherSourceHash
} | ConvertTo-Json -Compress
