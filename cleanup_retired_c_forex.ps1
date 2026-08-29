[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Target = [IO.Path]::GetFullPath(
    "C:\Users\zmoor\Documents\forex"
).TrimEnd("\")
$Expected = "C:\Users\zmoor\Documents\forex"
$LogPath = "D:\forex\runtime\retired_c_forex_cleanup.log"

function Write-CleanupLog {
    param([string]$Message)
    $parent = Split-Path -Parent $LogPath
    if (-not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value (
        "{0} {1}" -f (Get-Date).ToUniversalTime().ToString("o"), $Message
    )
}

if ($Target -ne $Expected) {
    throw "Resolved cleanup target mismatch: $Target"
}
if (-not (Test-Path -LiteralPath $Target)) {
    Write-CleanupLog "target already absent"
    exit 0
}

$rootItem = Get-Item -LiteralPath $Target -Force
if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw "Refusing to remove reparse point: $Target"
}

$allowedRootNames = @(".pytest_cache", "trad")
$unexpectedRoot = @(
    Get-ChildItem -LiteralPath $Target -Force |
        Where-Object { $_.Name -notin $allowedRootNames }
)
if ($unexpectedRoot.Count -gt 0) {
    throw "Unexpected entries remain in retired root: $($unexpectedRoot.Name -join ', ')"
}

$tradPath = Join-Path $Target "trad"
if (Test-Path -LiteralPath $tradPath) {
    $unexpectedTrad = @(
        Get-ChildItem -LiteralPath $tradPath -Force |
            Where-Object { $_.Name -ne ".pytest_cache" }
    )
    if ($unexpectedTrad.Count -gt 0) {
        throw "Unexpected entries remain in retired trad root: $($unexpectedTrad.Name -join ', ')"
    }
}

Remove-Item -LiteralPath $Target -Recurse -Force
if (Test-Path -LiteralPath $Target) {
    throw "Retired C Forex root still exists after cleanup"
}
Write-CleanupLog "removed retired C Forex cache shells"
