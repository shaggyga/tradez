param(
    [ValidateSet('status','run','resume','verify','inspect')]
    [string]$Action = 'status',
    [string]$Config = (Join-Path (Split-Path -Parent $PSScriptRoot) 'FOREX_INSPECTOR_LOCAL.json'),
    [string]$Query
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    $forexConfigPath = (Resolve-Path -LiteralPath $Config).Path
    $forexLocal = Get-Content -LiteralPath $forexConfigPath -Raw | ConvertFrom-Json
    $forexRecipe = Join-Path $PSScriptRoot 'CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json'
    $forexActualHash = (Get-FileHash -LiteralPath $forexRecipe -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($forexActualHash -cne $forexLocal.recipe_sha256) {
        throw 'Pinned recipe changed. Preserve evidence; do not regenerate the approval.'
    }
    if (-not (Test-Path -LiteralPath $forexLocal.python -PathType Leaf)) {
        throw 'Configured Python runtime is missing. Restore the documented dependency environment.'
    }
    $forexArguments = @('-I', '-B', (Join-Path $PSScriptRoot 'campaign_inspector_operator_v2.py'),
        $Action, '--recipe', $forexRecipe, '--recipe-sha256', $forexLocal.recipe_sha256,
        '--paths', $forexLocal.paths, '--runs-dir', $forexLocal.runs_dir)
    if ($Action -eq 'inspect') {
        if ([string]::IsNullOrWhiteSpace($Query)) { throw 'Inspect requires an explicit original-record query JSON file.' }
        $forexArguments += @('--query', (Resolve-Path -LiteralPath $Query).Path)
    }
    & $forexLocal.python @forexArguments
    $forexExitCode = $LASTEXITCODE
    exit $forexExitCode
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 2
}
