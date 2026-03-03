# Fix Python 3.11 environment for Windows (system PATH)
# Requires admin privileges

$ErrorActionPreference = 'Stop'

$pyExe = py -3.11 -c "import sys; print(sys.executable)"
$pyDir = Split-Path $pyExe -Parent
$scriptsDir = Join-Path $pyDir 'Scripts'

$machinePath = [Environment]::GetEnvironmentVariable('Path','Machine')
if (-not $machinePath) {
    $machinePath = ''
}

$parts = $machinePath -split ';' | Where-Object { $_ -and $_.Trim() -ne '' }
$parts = $parts | Where-Object { $_ -ne 'C:\Users\cgf\AppData\Local\Microsoft\WindowsApps' }
$parts = $parts | Where-Object { $_ -ne $pyDir -and $_ -ne $scriptsDir }

$seen = @{}
$dedup = New-Object System.Collections.Generic.List[string]
foreach ($p in $parts) {
    $k = $p.TrimEnd('\\')
    if (-not $seen.ContainsKey($k)) {
        $seen[$k] = $true
        $dedup.Add($p)
    }
}

$newPath = @($pyDir, $scriptsDir) + $dedup
[Environment]::SetEnvironmentVariable('Path', ($newPath -join ';'), 'Machine')

Write-Output "Machine PATH updated with Python 3.11 first:"
Write-Output $pyDir
Write-Output $scriptsDir
