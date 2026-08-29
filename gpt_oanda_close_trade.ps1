param(
  [Parameter(Mandatory = $true)][string]$TradeId,
  [string]$Units = "ALL",
  [string]$CredsPath = ".\trad\creds"
)

$ErrorActionPreference = "Stop"

$creds = Get-Content -Raw -Path $CredsPath

function Get-Cfg {
  param([string[]]$Names)
  foreach ($name in $Names) {
    $envValue = [Environment]::GetEnvironmentVariable($name)
    if ($envValue) {
      return $envValue.Trim()
    }
    $pattern = "(?m)^\s*" + [regex]::Escape($name) + "\s*=\s*[""']?([^""'\r\n#]+)[""']?"
    $match = [regex]::Match($creds, $pattern)
    if ($match.Success) {
      return $match.Groups[1].Value.Trim()
    }
  }
  return ""
}

$token = Get-Cfg @("OANDA_API_KEY", "OANDA_API_TOKEN")
$accountId = Get-Cfg @("OANDA_ACCOUNT_ID_GPT", "OANDA_ACCOUNT_ID_MAJ")
if (-not $token -or -not $accountId) {
  throw "Missing OANDA practice token or GPT account id in creds."
}

$base = "https://api-fxpractice.oanda.com"
$headers = @{
  Authorization = "Bearer $token"
  "Content-Type" = "application/json"
}

$body = @{ units = $Units } | ConvertTo-Json
$response = Invoke-RestMethod -Headers $headers -Method Put -Uri "$base/v3/accounts/$accountId/trades/$TradeId/close" -Body $body -TimeoutSec 20
$response | ConvertTo-Json -Depth 10
