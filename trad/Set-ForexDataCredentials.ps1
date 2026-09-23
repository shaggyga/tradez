param(
    [switch]$TradingEconomics,
    [switch]$Fred
)

$ErrorActionPreference = "Stop"

function Read-SecretText([string]$Prompt) {
    $secure = Read-Host -Prompt $Prompt -AsSecureString
    $credential = [System.Management.Automation.PSCredential]::new("unused", $secure)
    return $credential.GetNetworkCredential().Password.Trim()
}

if (-not $TradingEconomics -and -not $Fred) {
    $TradingEconomics = $true
    $Fred = $true
}

$changed = @()
if ($TradingEconomics) {
    $value = Read-SecretText "Trading Economics API key (client:secret)"
    if ([string]::IsNullOrWhiteSpace($value) -or $value -notmatch ':') {
        throw "Trading Economics key must use the provider's client:secret format."
    }
    [Environment]::SetEnvironmentVariable("TRADING_ECONOMICS_API_KEY", $value, "User")
    $changed += "TRADING_ECONOMICS_API_KEY"
    Remove-Variable value -ErrorAction SilentlyContinue
}

if ($Fred) {
    $value = Read-SecretText "FRED API key (32 lowercase letters/digits)"
    if ($value -notmatch '^[a-z0-9]{32}$') {
        throw "FRED API key must be exactly 32 lowercase letters/digits."
    }
    [Environment]::SetEnvironmentVariable("FRED_API_KEY", $value, "User")
    $changed += "FRED_API_KEY"
    Remove-Variable value -ErrorAction SilentlyContinue
}

Write-Host ("Saved to the current user's environment: " + ($changed -join ", "))
Write-Host "The values were not printed. Tell Codex 'credentials are set' so the hidden supervisor can be safely reloaded and the adapters validated."
