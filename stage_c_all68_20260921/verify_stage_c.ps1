$ErrorActionPreference = 'Stop'
$stageRoot = Split-Path -Parent $PSCommandPath
$python = 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'

& $python -I -B (Join-Path $stageRoot 'verify_stage_c_handoff.py')
& $python -I -B (Join-Path $stageRoot 'all68_run_preflight.py')
& $python -I -B -m pytest -q --noconftest `
    (Join-Path $stageRoot 'test_all68_global_clock.py') `
    (Join-Path $stageRoot 'test_all68_execution_accounting.py') `
    (Join-Path $stageRoot 'test_endpoint_targets.py') `
    (Join-Path $stageRoot 'test_all68_endpoint_baseline.py') `
    (Join-Path $stageRoot 'test_calendar_targets.py') `
    (Join-Path $stageRoot 'test_stage_c_offline_isolation.py')

Write-Output 'Stage C verification passed: offline only; models denied; no execution readiness.'
