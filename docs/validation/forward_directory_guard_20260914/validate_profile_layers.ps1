param([string]$CaseFile,[string]$CanonicalRoot)
$ErrorActionPreference='Stop'
$cases=Get-Content -LiteralPath $CaseFile -Raw | ConvertFrom-Json
. (Join-Path $CanonicalRoot 'oanda_operational_recovery_contract.ps1')
$source=[IO.File]::ReadAllText((Join-Path $CanonicalRoot 'oanda_always_on_supervisor.ps1'))
$start=$source.IndexOf('function Get-OperationalForwardNeedle {')
$end=$source.IndexOf('$SupervisorStartedUtc =')
if ($start -lt 0 -or $end -le $start) { throw 'Bounded supervisor validation extent not found.' }
$validation=$source.Substring($start,$end-$start)
if ($validation -match 'Start-Process|Stop-Process|Write-SupervisorEvent|Register-ScheduledTask') {
    throw 'Inert supervisor validation boundary violated.'
}
$block=[ScriptBlock]::Create($validation)
function Test-SupervisorProfile([string]$Project,[string]$Profile) {
    $Trad=$Project
    $DataRoot=Join-Path $Trad 'data\oanda_training_manager'
    $OperationalProfilePath=$Profile
    $ResearchCollectionOnly=$true
    $SafeCoreOnly=$true
    $DisabledNames=@()
    $ResearchCollectionNames=@()
    . $block
    if (-not $OperationalProfileSha256) { throw 'No supervisor profile binding returned.' }
}
$results=@()
foreach ($case in $cases) {
    foreach ($layer in @('recovery','supervisor')) {
        $accepted=$false;$reason=''
        try {
            if ($layer -eq 'recovery') {
                $null=Read-OperationalRecoveryProfile -Trad $case.project -ProfilePath $case.path -RecoveryUntilUtc '2026-09-20T23:59:00Z' -Now ([DateTimeOffset]'2026-09-14T02:45:00Z')
            } else {
                Test-SupervisorProfile -Project $case.project -Profile $case.path
            }
            $accepted=$true
        } catch { $reason=$_.Exception.Message }
        $results+=@{case=$case.name;layer=$layer;accepted=$accepted;expected=$case.accept;reason=$reason}
        if ($accepted -ne $case.accept) {
            $results | ConvertTo-Json -Depth 5
            throw "Unexpected validation result: $($case.name) / $layer / $reason"
        }
    }
}
$results | ConvertTo-Json -Depth 5
