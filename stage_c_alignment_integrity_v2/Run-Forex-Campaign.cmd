@echo off
setlocal
set "FOREX_CAMPAIGN_CONFIG=%~dp0..\FOREX_INSPECTOR_LOCAL.json"
for /f "usebackq delims=" %%P in (`powershell.exe -NoProfile -Command "(ConvertFrom-Json -InputObject (Get-Content -LiteralPath $env:FOREX_CAMPAIGN_CONFIG -Raw)).python"`) do set "FOREX_CAMPAIGN_PYTHON=%%P"
if not defined FOREX_CAMPAIGN_PYTHON exit /b 2
"%FOREX_CAMPAIGN_PYTHON%" -I -B "%~dp0campaign_launcher_v2.py" %*
exit /b %errorlevel%
