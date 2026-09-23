[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Destination,
    [string]$VaultProject = "$HOME\OneDrive\thevault\projects\forex",
    [string]$Python = "python",
    [string]$ValidationProfile = "model_gap_h24_recovery",
    [ValidateSet("evidence", "smoke", "full")]
    [string]$ValidationMode = "evidence"
)

$ErrorActionPreference = "Stop"
$pointerPath = Join-Path $VaultProject "CHECKPOINT_LATEST.json"
$importerPath = Join-Path $VaultProject "forex_vault_import.py"
if (-not (Test-Path -LiteralPath $pointerPath -PathType Leaf)) {
    throw "Checkpoint pointer not found: $pointerPath"
}
if (-not (Test-Path -LiteralPath $importerPath -PathType Leaf)) {
    throw "Standalone importer not found: $importerPath"
}

$pointer = Get-Content -Raw -LiteralPath $pointerPath | ConvertFrom-Json
$archivePath = Join-Path $VaultProject $pointer.base_checkpoint.archive
$expectedHash = [string]$pointer.base_checkpoint.archive_sha256
if (-not (Test-Path -LiteralPath $archivePath -PathType Leaf)) {
    throw "Checkpoint archive not found: $archivePath"
}
$observedHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($observedHash -ne $expectedHash.ToLowerInvariant()) {
    throw "Checkpoint SHA-256 mismatch. Expected $expectedHash, observed $observedHash"
}

& $Python $importerPath `
    --base $archivePath `
    --destination $Destination `
    --expected-sha256 $expectedHash
if ($LASTEXITCODE -ne 0) {
    throw "Vault import failed with exit code $LASTEXITCODE"
}

$validator = Join-Path $Destination "trad\forex_validation_recreation.py"
& $Python $validator `
    --root $Destination `
    --profile $ValidationProfile `
    --mode $ValidationMode
if ($LASTEXITCODE -ne 0) {
    throw "Validation recreation failed with exit code $LASTEXITCODE"
}

[pscustomobject]@{
    Status = "imported_and_validated"
    Destination = (Resolve-Path -LiteralPath $Destination).Path
    Archive = $archivePath
    ArchiveSha256 = $expectedHash
    ValidationProfile = $ValidationProfile
    ValidationMode = $ValidationMode
}
