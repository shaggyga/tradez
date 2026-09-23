$ErrorActionPreference = 'Stop'
$ProjectRoot = 'C:\Users\zmoor\Documents\forex\trad'
$EvidenceRoot = $PSScriptRoot
$Bootstrap = Join-Path $ProjectRoot 'start_oanda_practice_recovery_v1.ps1'
$PowerShellExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$TaskName = 'Forex Practice Research Recovery 20260910'
$RunKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
$RunName = 'ForexPracticeResearchRecovery20260910'
$Arguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $Bootstrap + '"'

# Inspect the concrete source-bound recovery action before registering it.
$CheckText = & $PowerShellExe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $Bootstrap -CheckOnly
if ($LASTEXITCODE -ne 0) { throw 'Recovery bootstrap preflight refused; no startup registration changed.' }
$Check = ($CheckText -join "`n") | ConvertFrom-Json
if ($Check.phase -ne 'already_running' -or -not $Check.check_only -or -not $Check.practice_only) {
    throw 'Expected the currently running practice/research services before registration.'
}
$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$Sid = $Identity.User.Value
$Existing = Get-ScheduledTask -TaskName $TaskName -TaskPath '\' -ErrorAction SilentlyContinue
if ($Existing) {
    Export-ScheduledTask -TaskName $TaskName -TaskPath '\' | Set-Content -LiteralPath (Join-Path $EvidenceRoot 'RECOVERY_TASK_BEFORE.xml') -Encoding UTF8
    if ($Existing.Actions.Count -ne 1 -or $Existing.Actions[0].Execute -ine $PowerShellExe -or
        $Existing.Actions[0].Arguments -cne $Arguments) { throw 'Existing task has a different action; it was not replaced.' }
}
$Method = $null
try {
    $Action = New-ScheduledTaskAction -Execute $PowerShellExe -Argument $Arguments -WorkingDirectory $ProjectRoot
    $Trigger = New-ScheduledTaskTrigger -AtLogOn -User $Sid
    $Trigger.Delay = 'PT30S'
    $Principal = New-ScheduledTaskPrincipal -UserId $Sid -LogonType Interactive -RunLevel Limited
    $Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval ([TimeSpan]::FromMinutes(1))
    Register-ScheduledTask -TaskName $TaskName -TaskPath '\' -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings -Description 'Restore the source-bound Forex research collection and finite OANDA Practice007 trial at this user sign-in. No real-money route. The trial keeps its September 11 cutoff and durable loss limits.' -Force | Out-Null
    $Actual = Get-ScheduledTask -TaskName $TaskName -TaskPath '\'
    if (-not $Actual.Settings.Enabled -or $Actual.Actions.Count -ne 1 -or
        $Actual.Actions[0].Execute -ine $PowerShellExe -or $Actual.Actions[0].Arguments -cne $Arguments -or
        $Actual.Principal.LogonType -ne 'Interactive' -or $Actual.Principal.RunLevel -ne 'Limited') { throw 'Task verification failed.' }
    Export-ScheduledTask -TaskName $TaskName -TaskPath '\' | Set-Content -LiteralPath (Join-Path $EvidenceRoot 'RECOVERY_TASK_INSTALLED.xml') -Encoding UTF8
    $Method = 'current_user_logon_task'
} catch {
    # No elevation/password request. A successfully created task is not followed
    # by a duplicate Run entry if a later verification step fails.
    if (Get-ScheduledTask -TaskName $TaskName -TaskPath '\' -ErrorAction SilentlyContinue) { throw 'A task exists but full verification failed; no duplicate startup entry created.' }
    $Previous = Get-ItemPropertyValue -LiteralPath $RunKey -Name $RunName -ErrorAction SilentlyContinue
    $Command = '"' + $PowerShellExe + '" ' + $Arguments
    if ($Previous -and $Previous -cne $Command) { throw 'Different existing user startup command was not replaced.' }
    New-ItemProperty -LiteralPath $RunKey -Name $RunName -Value $Command -PropertyType String -Force | Out-Null
    if ((Get-ItemPropertyValue -LiteralPath $RunKey -Name $RunName) -cne $Command) { throw 'User startup verification failed.' }
    $Method = 'current_user_logon_run_key'
}
$Receipt = @{ observed_utc=[DateTime]::UtcNow.ToString('o'); method=$Method; task_name=if($Method -eq 'current_user_logon_task'){$TaskName}else{$null}; run_value_name=if($Method -eq 'current_user_logon_run_key'){$RunName}else{$null}; executable=$PowerShellExe; arguments=$Arguments; bootstrap=$Bootstrap; practice_only=$true; current_user_only=$true; logon_required=$true; before_login_boot_execution=$false; preflight=$Check }
$Receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $EvidenceRoot 'LOGON_RECOVERY_INSTALLED_20260910.json') -Encoding UTF8
[pscustomobject]$Receipt | Select-Object observed_utc,method,task_name,run_value_name,practice_only,logon_required | ConvertTo-Json -Compress
