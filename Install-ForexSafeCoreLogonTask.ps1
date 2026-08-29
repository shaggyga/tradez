param(
    [switch]$ValidateOnly,
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"

$TaskName = "ForexSafeCoreAtLogon"
$TaskPath = "\"
$CanonicalRoot = "C:\Users\zmoor\Documents\forex"
$Trad = Join-Path $CanonicalRoot "trad"
$Launcher = Join-Path $Trad "start_oanda_safe_core.ps1"
$PowerShell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$CurrentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$CurrentUser = $CurrentIdentity.Name
$CurrentUserSid = $CurrentIdentity.User.Value

if (-not (Test-Path -LiteralPath $Launcher -PathType Leaf)) {
    throw "Canonical safe-core launcher is missing: $Launcher"
}
if (-not (Test-Path -LiteralPath $PowerShell -PathType Leaf)) {
    throw "Windows PowerShell is missing: $PowerShell"
}

$ExpectedArguments = @(
    "-NoLogo"
    "-NoProfile"
    "-NonInteractive"
    "-WindowStyle", "Hidden"
    "-ExecutionPolicy", "Bypass"
    "-File", ('"{0}"' -f $Launcher)
    "-Root", ('"{0}"' -f $CanonicalRoot)
) -join " "

function Get-ForexTask {
    Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -ErrorAction SilentlyContinue
}

function Test-ForexTaskDefinition {
    param([Parameter(Mandatory)]$Task)

    $action = @($Task.Actions)[0]
    $trigger = @($Task.Triggers)[0]
    $errors = @()
    $taskUserSid = try {
        ([System.Security.Principal.NTAccount]::new(
            [string]$Task.Principal.UserId
        )).Translate(
            [System.Security.Principal.SecurityIdentifier]
        ).Value
    } catch {
        ""
    }
    if (@($Task.Actions).Count -ne 1) { $errors += "expected_one_action" }
    if (@($Task.Triggers).Count -ne 1) { $errors += "expected_one_trigger" }
    if ($action.Execute -ine $PowerShell) { $errors += "wrong_executable" }
    if ($action.Arguments -cne $ExpectedArguments) { $errors += "wrong_arguments" }
    if ($action.WorkingDirectory -ine $Trad) { $errors += "wrong_working_directory" }
    if ($taskUserSid -ne $CurrentUserSid) { $errors += "wrong_user" }
    if ([string]$Task.Principal.LogonType -ne "Interactive") { $errors += "wrong_logon_type" }
    if ([string]$Task.Principal.RunLevel -ne "Limited") { $errors += "wrong_run_level" }
    if (-not [bool]$Task.Settings.Hidden) { $errors += "task_not_hidden" }
    if ([string]$Task.Settings.MultipleInstances -ne "IgnoreNew") { $errors += "multiple_instances_not_ignored" }
    if ($trigger.CimClass.CimClassName -ne "MSFT_TaskLogonTrigger") { $errors += "wrong_trigger" }

    [pscustomobject]@{
        schema_version = 1
        task_name = $TaskName
        task_path = $TaskPath
        user = $CurrentUser
        user_sid = $CurrentUserSid
        registered_user = [string]$Task.Principal.UserId
        canonical_root = $CanonicalRoot
        launcher = $Launcher
        executable = $PowerShell
        arguments = $ExpectedArguments
        hidden = [bool]$Task.Settings.Hidden
        enabled = [bool]$Task.Settings.Enabled
        state = [string]$Task.State
        valid = ($errors.Count -eq 0)
        errors = $errors
    }
}

if ($Uninstall) {
    $existing = Get-ForexTask
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Confirm:$false
    }
    [pscustomobject]@{
        task_name = $TaskName
        removed = ($null -ne $existing)
        exists = ($null -ne (Get-ForexTask))
    } | ConvertTo-Json -Depth 4
    exit 0
}

if (-not $ValidateOnly) {
    $action = New-ScheduledTaskAction `
        -Execute $PowerShell `
        -Argument $ExpectedArguments `
        -WorkingDirectory $Trad
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
    $principal = New-ScheduledTaskPrincipal `
        -UserId $CurrentUser `
        -LogonType Interactive `
        -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet `
        -Hidden `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew
    $definition = New-ScheduledTask `
        -Action $action `
        -Trigger $trigger `
        -Principal $principal `
        -Settings $settings `
        -Description "Start the canonical Forex safe-core supervisor hidden at current-user logon. The launcher is idempotent and does nothing when the supervisor is already running."
    Register-ScheduledTask `
        -TaskName $TaskName `
        -TaskPath $TaskPath `
        -InputObject $definition `
        -Force | Out-Null
}

$task = Get-ForexTask
if ($null -eq $task) {
    throw "Scheduled task is not installed: $TaskPath$TaskName"
}
$validation = Test-ForexTaskDefinition -Task $task
$validation | ConvertTo-Json -Depth 5
if (-not $validation.valid) {
    exit 1
}
