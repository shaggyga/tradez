param(
  [string]$SinceId = "",
  [int]$Count = 20,
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

if ($SinceId) {
  $uri = "$base/v3/accounts/$accountId/transactions/sinceid?id=$SinceId"
  $response = Invoke-RestMethod -Headers $headers -Uri $uri -TimeoutSec 20
  $transactions = @($response.transactions)
} else {
  $summary = Invoke-RestMethod -Headers $headers -Uri "$base/v3/accounts/$accountId/summary" -TimeoutSec 20
  $lastId = [int]$summary.account.lastTransactionID
  $fromId = [math]::Max(1, $lastId - $Count + 1)
  $uri = "$base/v3/accounts/$accountId/transactions/idrange?from=$fromId&to=$lastId"
  $response = Invoke-RestMethod -Headers $headers -Uri $uri -TimeoutSec 20
  $transactions = @($response.transactions)
}

$transactions |
  Select-Object id, time, type, instrument, units, price, pl, financing, accountBalance, reason, orderID, tradeID, replacedByOrderID, tradeOpened, tradesClosed |
  ConvertTo-Json -Depth 8
