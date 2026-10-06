<#
.SYNOPSIS
    Rewrites AgentOS-era identifiers in a tree to their Fenced equivalents.

.DESCRIPTION
    Migrates configuration, deployment, and source files after upgrading to a
    Fenced build. Rewrites:

        AGENTOS_*   -> FENCED_*
        AgentOS     -> Fenced
        Agentos     -> Fenced
        agentos     -> fenced

    Binary files are skipped. .git, node_modules, __pycache__, and vendor
    directories are excluded. Only file contents are rewritten; directories and
    binaries are not renamed. See docs/COMPATIBILITY.md for the full boundary.

.PARAMETER Path
    Directory or file to migrate. Defaults to the current directory.

.PARAMETER DryRun
    Report what would change without writing anything.

.PARAMETER Quiet
    Only print the final summary.

.EXAMPLE
    ./scripts/migrate-agentos-to-fenced.ps1 -DryRun ./deploy
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Path = ".",

    [switch]$DryRun,

    [switch]$Quiet
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    Write-Error "migrate-agentos-to-fenced: path not found: $Path"
    exit 1
}

$excludeDirs = @('.git', 'node_modules', '__pycache__', 'vendor')
$patterns = @('AgentOS', 'AGENTOS', 'Agentos', 'agentos')
$replacements = @('Fenced', 'FENCED', 'Fenced', 'fenced')

$target = Get-Item -LiteralPath $Path
if ($target.PSIsContainer) {
    $candidates = Get-ChildItem -LiteralPath $target.FullName -Recurse -File
} else {
    $candidates = @($target)
}

$files = foreach ($candidate in $candidates) {
    $relative = $candidate.FullName.Substring($target.FullName.Length).TrimStart('\', '/')
    $segments = $relative -split '[\\/]'
    $excluded = $false
    foreach ($segment in $segments) {
        if ($excludeDirs -contains $segment) { $excluded = $true; break }
    }
    if (-not $excluded) { $candidate }
}

$results = New-Object System.Collections.Generic.List[object]

foreach ($file in $files) {
    $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
    $probe = [Math]::Min(8192, $bytes.Length)
    $isBinary = $false
    for ($i = 0; $i -lt $probe; $i++) {
        if ($bytes[$i] -eq 0) { $isBinary = $true; break }
    }
    if ($isBinary) { continue }

    $text = [System.IO.File]::ReadAllText($file.FullName)
    $hits = 0
    foreach ($pattern in $patterns) {
        $hits += ([regex]::Matches($text, [regex]::Escape($pattern))).Count
    }
    if ($hits -eq 0) { continue }

    $results.Add([pscustomobject]@{
        File = $file.FullName
        Hits = $hits
        Text = $text
    })
}

if ($results.Count -eq 0) {
    Write-Host "migrate-agentos-to-fenced: nothing to migrate under $Path"
    exit 0
}

$mode = if ($DryRun) { "DRY RUN" } else { "rewriting" }
Write-Host "migrate-agentos-to-fenced: $mode $($results.Count) file(s) under $Path"
Write-Host ""

$hitsTotal = 0
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

foreach ($result in $results) {
    $hitsTotal += $result.Hits
    if (-not $Quiet) {
        Write-Host ("  {0,5}  {1}" -f $result.Hits, $result.File)
    }
    if (-not $DryRun) {
        $updated = $result.Text
        for ($i = 0; $i -lt $patterns.Count; $i++) {
            $updated = $updated.Replace($patterns[$i], $replacements[$i])
        }
        [System.IO.File]::WriteAllText($result.File, $updated, $utf8NoBom)
    }
}

Write-Host ""
if ($DryRun) {
    Write-Host "summary: $($results.Count) file(s), $hitsTotal replacement(s) pending (dry run, nothing written)"
} else {
    Write-Host "summary: $($results.Count) file(s), $hitsTotal replacement(s) applied"
    Write-Host ""
    Write-Host "next steps:"
    Write-Host "  1. review the diff (git diff)"
    Write-Host "  2. rename AGENTOS_* variables in your secret store and CI settings"
    Write-Host "  3. redeploy: gRPC clients and servers must both be rebuilt, because the"
    Write-Host "     protobuf package changed and the wire protocol is not backward compatible"
}
