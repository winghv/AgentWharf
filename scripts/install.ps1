$ErrorActionPreference = "Stop"

function Get-EnvOrDefault([string]$Name, [string]$Fallback) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrEmpty($value)) { return $Fallback }
    return $value
}

function Say([string]$Message) {
    [Console]::Error.WriteLine("wharf: $Message")
}

$repo = Get-EnvOrDefault "AGENTWHARF_REPO" "winghv/agentwharf"
$version = Get-EnvOrDefault "AGENTWHARF_VERSION" "latest"
$providerDir = Get-EnvOrDefault "AGENTWHARF_PROVIDER_DIR" (Join-Path $HOME ".agentwharf/providers")
$claudeAcpPackage = Get-EnvOrDefault "AGENTWHARF_CLAUDE_ACP_PACKAGE" "@agentclientprotocol/claude-agent-acp@0.54.1"
$codexAcpPackage = Get-EnvOrDefault "AGENTWHARF_CODEX_ACP_PACKAGE" "@agentclientprotocol/codex-acp@1.0.2"

$existing = Get-Command wharf.exe -ErrorAction SilentlyContinue
if ($null -eq $existing) { $existing = Get-Command wharf -ErrorAction SilentlyContinue }
$existingInstallDir = $null
if ($null -ne $existing -and -not [string]::IsNullOrEmpty($existing.Source)) {
    $existingInstallDir = Split-Path -Parent ([IO.Path]::GetFullPath($existing.Source))
}

$installDirOverride = [Environment]::GetEnvironmentVariable("AGENTWHARF_INSTALL_DIR")
if (-not [string]::IsNullOrEmpty($installDirOverride)) {
    $installDir = $installDirOverride
} elseif (-not [string]::IsNullOrEmpty($existingInstallDir)) {
    $installDir = $existingInstallDir
} else {
    $installDir = Join-Path $HOME ".local/bin"
}

$installMode = "install"
if (-not [string]::IsNullOrEmpty($existingInstallDir) -and
    ([IO.Path]::GetFullPath($installDir) -eq [IO.Path]::GetFullPath($existingInstallDir))) {
    $installMode = "upgrade"
}

$machineArch = [Environment]::GetEnvironmentVariable("PROCESSOR_ARCHITEW6432")
if ([string]::IsNullOrEmpty($machineArch)) { $machineArch = [Environment]::GetEnvironmentVariable("PROCESSOR_ARCHITECTURE") }
switch ($machineArch.ToUpperInvariant()) {
    "AMD64" { $arch = "amd64" }
    "ARM64" { $arch = "arm64" }
    default { throw "unsupported architecture: $machineArch" }
}

$os = Get-EnvOrDefault "AGENTWHARF_OS" "windows"
$arch = Get-EnvOrDefault "AGENTWHARF_ARCH" $arch
if ($os -ne "windows" -or ($arch -ne "amd64" -and $arch -ne "arm64")) {
    throw "unsupported target: $os-$arch"
}

$asset = "agentwharf-$os-$arch.zip"
if ($version -eq "latest") {
    $releaseBase = "https://github.com/$repo/releases/latest/download"
} else {
    $releaseBase = "https://github.com/$repo/releases/download/$version"
}
$releaseBase = Get-EnvOrDefault "AGENTWHARF_RELEASE_BASE" $releaseBase
$assetUrl = "$releaseBase/$asset"
$checksumUrl = "$releaseBase/checksums.txt"

if ([Environment]::GetEnvironmentVariable("AGENTWHARF_INSTALL_DRY_RUN") -eq "1") {
    "asset_url=$assetUrl"
    "checksum_url=$checksumUrl"
    "install_mode=$installMode"
    if ($null -ne $existing) { "existing_wharf=$($existing.Source)" }
    "install_wharf=$(Join-Path $installDir 'wharf.exe')"
    "provider_dir=$providerDir"
    "provider_package=$claudeAcpPackage"
    "provider_package=$codexAcpPackage"
    exit 0
}

if ($installMode -eq "upgrade") { Say "upgrading existing Wharf in $installDir" }
else { Say "installing Wharf in $installDir" }

$tempDir = Join-Path ([IO.Path]::GetTempPath()) ("agentwharf." + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
try {
    $archive = Join-Path $tempDir $asset
    $checksums = Join-Path $tempDir "checksums.txt"
    $extractDir = Join-Path $tempDir "extract"

    Say "downloading $assetUrl"
    Invoke-WebRequest -Uri $assetUrl -OutFile $archive -UseBasicParsing
    Invoke-WebRequest -Uri $checksumUrl -OutFile $checksums -UseBasicParsing

    $checksumLine = Get-Content $checksums | Where-Object { $_ -match "^\s*([0-9a-fA-F]{64})\s{2}([^	 ]+)\s*$" -and $Matches[2] -eq $asset } | Select-Object -First 1
    if ($null -eq $checksumLine) { throw "checksum for $asset not found" }
    $expected = ([regex]::Match($checksumLine, "^\s*([0-9a-fA-F]{64})")).Groups[1].Value.ToLowerInvariant()
    $actual = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $expected) { throw "checksum mismatch for $asset" }

    Expand-Archive -LiteralPath $archive -DestinationPath $extractDir -Force
    $binary = Join-Path $extractDir "agentwharf.exe"
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) { throw "release archive does not contain agentwharf.exe" }

    if ([Environment]::GetEnvironmentVariable("AGENTWHARF_SKIP_PROVIDER_BRIDGES") -ne "1") {
        $npm = Get-Command npm -ErrorAction SilentlyContinue
        if ($null -eq $npm) { throw "missing required command: npm" }
        Say "installing ACP provider bridges in $providerDir"
        New-Item -ItemType Directory -Path $providerDir -Force | Out-Null
        & $npm.Source install --prefix $providerDir --omit=dev $claudeAcpPackage $codexAcpPackage | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "npm provider bridge installation failed" }
    }

    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
    Copy-Item -LiteralPath $binary -Destination (Join-Path $installDir "wharf.exe") -Force
    if ([Environment]::GetEnvironmentVariable("AGENTWHARF_SKIP_PROVIDER_BRIDGES") -ne "1") {
        foreach ($bridge in @("claude-agent-acp", "codex-acp")) {
            $bridgePath = Join-Path $providerDir "node_modules/.bin/$bridge.cmd"
            if (-not (Test-Path -LiteralPath $bridgePath -PathType Leaf)) { throw "$bridge was not installed" }
            Copy-Item -LiteralPath $bridgePath -Destination (Join-Path $installDir "$bridge.cmd") -Force
        }
    }

    Say "installed $(Join-Path $installDir 'wharf.exe')"
    if (($env:Path -split ';') -notcontains $installDir) {
        Say "$installDir is not on PATH; add it before running wharf"
    }
} finally {
    Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
