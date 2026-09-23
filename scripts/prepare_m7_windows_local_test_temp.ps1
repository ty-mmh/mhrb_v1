$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$nonce = New-Object byte[] 16
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $rng.GetBytes($nonce)
}
finally {
    $rng.Dispose()
}
$discriminator = -join @($nonce | ForEach-Object { $_.ToString('x2') })
$basename = "mahoroba-m7-local-$discriminator"

$programFiles = [System.Environment]::GetFolderPath([System.Environment+SpecialFolder]::ProgramFiles)
$go = Join-Path -Path $programFiles -ChildPath 'Go\bin\go.exe'
$goInfo = Get-Item -LiteralPath $go -Force
if (-not $goInfo.PSIsContainer -and $goInfo.Length -gt 0 -and ($goInfo.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0) {
    $output = @(& $go run ./cmd/mahoroba-ci-windows-test-temp --basename $basename)
}
else {
    throw 'the exact Program Files Go executable is not a direct nonempty file'
}
if ($LASTEXITCODE -ne 0) {
    throw "protected Windows local test temp helper failed with exit code $LASTEXITCODE"
}
$paths = @($output | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($paths.Count -ne 1 -or -not [System.IO.Path]::IsPathRooted($paths[0])) {
    throw 'protected Windows local test temp helper returned an invalid path'
}

# The caller owns process environment mutation. Emit exactly one random,
# handle-validated protected parent. The execution-set descriptor is then
# initialized create-new in an absent child, avoiding any descriptor-ID/root
# naming cycle. TMP/TEMP/TMPDIR/GOTMPDIR and the strict marker are bound to
# this parent before a Windows suite starts.
Write-Output -NoEnumerate $paths[0]
