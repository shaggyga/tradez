param(
  [Parameter(Mandatory = $true)][string]$Instrument,
  [Parameter(Mandatory = $true)][int]$Units,
  [Parameter(Mandatory = $true)][string]$TakeProfit,
  [Parameter(Mandatory = $true)][string]$StopLoss,
  [Parameter(Mandatory = $true)][string]$TrailingDistance,
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

$order = @{
  order = @{
    type = "MARKET"
    instrument = $Instrument
    units = [string]$Units
    timeInForce = "FOK"
    positionFill = "DEFAULT"
    takeProfitOnFill = @{
      price = $TakeProfit
      timeInForce = "GTC"
    }
    stopLossOnFill = @{
      price = $StopLoss
      timeInForce = "GTC"
    }
    trailingStopLossOnFill = @{
      distance = $TrailingDistance
      timeInForce = "GTC"
    }
  }
}

$body = $order | ConvertTo-Json -Depth 8
$response = Invoke-RestMethod -Headers $headers -Method Post -Uri "$base/v3/accounts/$accountId/orders" -Body $body -TimeoutSec 20
$response | ConvertTo-Json -Depth 10
