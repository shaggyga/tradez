#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"

Write-Host "Configuring Windows Time for this workgroup machine..."
Set-Service -Name W32Time -StartupType Automatic
if ((Get-Service -Name W32Time).Status -ne "Running") {
    Start-Service -Name W32Time
}

# Client mode (0x8) avoids symmetric-active incompatibilities. Use the Windows
# service endpoint rather than weakening any application clock gate.
& w32tm /config /manualpeerlist:"time.windows.com,0x8" /syncfromflags:manual /update
if ($LASTEXITCODE -ne 0) { throw "w32tm configuration failed: $LASTEXITCODE" }

Restart-Service -Name W32Time
& w32tm /resync /rediscover
if ($LASTEXITCODE -ne 0) { throw "w32tm resync failed: $LASTEXITCODE" }

Start-Sleep -Seconds 3
Write-Host "Windows Time status:"
& w32tm /query /status
if ($LASTEXITCODE -ne 0) { throw "w32tm status query failed: $LASTEXITCODE" }

Write-Host "Service state:"
Get-Service -Name W32Time | Select-Object Status, StartType, Name
Write-Host "Time repair complete. Tell Codex 'time fixed' so the application gate can be verified against broker time."
