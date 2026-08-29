param(
  [string[]]$Instruments = @("EUR_NZD", "EUR_AUD", "USD_ZAR", "EUR_USD", "GBP_USD"),
  [string]$CredsPath = ".\trad\creds"
)

$ErrorActionPreference = "Stop"

$Instruments = @(
  foreach ($item in $Instruments) {
    foreach ($part in ($item -split ",")) {
      $clean = $part.Trim()
      if ($clean) { $clean }
    }
  }
)

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

function Get-PipSize {
  param([string]$Instrument)
  if ($Instrument -match "JPY") { return 0.01 }
  if ($Instrument -match "^XAU|^XAG") { return 0.01 }
  return 0.0001
}

function Get-Bps {
  param([double]$Start, [double]$End)
  if ($Start -eq 0.0) { return 0.0 }
  return (($End / $Start) - 1.0) * 10000.0
}

function Get-Candles {
  param(
    [string]$Instrument,
    [string]$Granularity,
    [int]$Count
  )
  $uri = "$base/v3/instruments/$Instrument/candles?price=M&granularity=$Granularity&count=$Count"
  $response = Invoke-RestMethod -Headers $headers -Uri $uri -TimeoutSec 20
  return @($response.candles | Where-Object { $_.complete })
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

$summary = Invoke-RestMethod -Headers $headers -Uri "$base/v3/accounts/$accountId/summary" -TimeoutSec 20
$trades = Invoke-RestMethod -Headers $headers -Uri "$base/v3/accounts/$accountId/openTrades" -TimeoutSec 20
$orders = Invoke-RestMethod -Headers $headers -Uri "$base/v3/accounts/$accountId/pendingOrders" -TimeoutSec 20
$pricing = Invoke-RestMethod -Headers $headers -Uri "$base/v3/accounts/$accountId/pricing?instruments=$($Instruments -join ',')" -TimeoutSec 20

$pricesByInstrument = @{}
foreach ($price in @($pricing.prices)) {
  $bid = if ($price.bids.Count -gt 0) { [double]$price.bids[0].price } else { $null }
  $ask = if ($price.asks.Count -gt 0) { [double]$price.asks[0].price } else { $null }
  $pricesByInstrument[$price.instrument] = [pscustomobject]@{
    bid = $bid
    ask = $ask
    mid = if ($null -ne $bid -and $null -ne $ask) { ($bid + $ask) / 2.0 } else { $null }
    spread_pips = if ($null -ne $bid -and $null -ne $ask) { [math]::Round(($ask - $bid) / (Get-PipSize $price.instrument), 2) } else { $null }
  }
}

$features = foreach ($instrument in $Instruments) {
  $m1 = Get-Candles $instrument "M1" 40
  $m5 = Get-Candles $instrument "M5" 30
  if ($m1.Count -lt 20 -or $m5.Count -lt 13) {
    continue
  }

  $last = [double]$m1[-1].mid.c
  $last20 = @($m1 | Select-Object -Last 20 | ForEach-Object { [double]$_.mid.c })
  $lo = ($last20 | Measure-Object -Minimum).Minimum
  $hi = ($last20 | Measure-Object -Maximum).Maximum
  $pos20 = if ($hi -gt $lo) { ($last - $lo) / ($hi - $lo) } else { 0.5 }
  $priceInfo = $pricesByInstrument[$instrument]

  [pscustomobject]@{
    instrument = $instrument
    m1_time = $m1[-1].time
    m5_time = $m5[-1].time
    bid = $priceInfo.bid
    ask = $priceInfo.ask
    live_mid = $priceInfo.mid
    spread_pips = $priceInfo.spread_pips
    last_m1_close = $last
    m1_r1_bp = [math]::Round((Get-Bps ([double]$m1[-2].mid.c) $last), 2)
    m1_r3_bp = [math]::Round((Get-Bps ([double]$m1[-4].mid.c) $last), 2)
    m1_r5_bp = [math]::Round((Get-Bps ([double]$m1[-6].mid.c) $last), 2)
    m1_r15_bp = [math]::Round((Get-Bps ([double]$m1[-16].mid.c) $last), 2)
    m5_r3_bp = [math]::Round((Get-Bps ([double]$m5[-4].mid.c) ([double]$m5[-1].mid.c)), 2)
    m5_r12_bp = [math]::Round((Get-Bps ([double]$m5[-13].mid.c) ([double]$m5[-1].mid.c)), 2)
    pos20 = [math]::Round($pos20, 2)
  }
}

[pscustomobject]@{
  local_time = Get-Date -Format o
  account = [pscustomobject]@{
    id_suffix = $accountId.Substring($accountId.Length - 4)
    balance = $summary.account.balance
    NAV = $summary.account.NAV
    marginUsed = $summary.account.marginUsed
    openTradeCount = $summary.account.openTradeCount
    unrealizedPL = $summary.account.unrealizedPL
  }
  openTrades = @($trades.trades | Select-Object id, instrument, currentUnits, price, unrealizedPL, realizedPL, openTime)
  pendingOrders = @($orders.orders | Select-Object id, type, tradeID, price, distance, state)
  live = @($features)
} | ConvertTo-Json -Depth 8
