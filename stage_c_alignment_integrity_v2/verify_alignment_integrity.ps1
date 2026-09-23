param(
    [string]$PythonExecutable = 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe',
    [string]$JunitXml = ''
)
$ErrorActionPreference = 'Stop'
$stageRoot = Split-Path -Parent $PSCommandPath
$python = $PythonExecutable

function Invoke-CheckedPython([string[]]$Arguments) {
    & $python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python verification failed with exit code $LASTEXITCODE"
    }
}

$verificationArguments = @('-I', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider', '--noconftest')
foreach ($testFile in @('test_alignment_integrity.py', 'test_publication_process_repair.py', 'test_session_calendar_repair.py', 'test_neutral_runner_repair.py', 'test_portable_checkpoint_v2.py', 'test_verifier_process_repair.py', 'test_quote_input_qualification_v2.py', 'test_conversion_input_qualification_v2.py', 'test_reference_accounting_adapter_v2.py', 'test_accounting_events_v2.py', 'test_accounting_event_runner_v2.py', 'test_resting_accounting_events_v2.py', 'test_accounting_stress_v2.py', 'test_accounting_checkpoint_v2.py')) {
    $verificationArguments += (Join-Path $stageRoot $testFile)
}
if ($JunitXml) { $verificationArguments += "--junitxml=$JunitXml" }
$verificationArguments += (Join-Path $stageRoot 'test_accounting_fastpath_v2.py')
$verificationArguments += (Join-Path $stageRoot 'test_forex_operator_v2.py')
$verificationArguments += (Join-Path $stageRoot 'test_accounting_review_repairs_v2.py')
$verificationArguments += (Join-Path $stageRoot 'test_policy_continuation_v2.py')
$verificationArguments += (Join-Path $stageRoot 'test_fitted_consumer_v2.py')
$verificationArguments += (Join-Path $stageRoot 'test_native_policy_input_v2.py')
Invoke-CheckedPython -Arguments $verificationArguments
Write-Output 'Alignment integrity verification passed: bounded contracts, process recovery and synthetic relocated replay; no model or policy evidence.'
