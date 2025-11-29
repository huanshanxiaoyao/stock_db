# PowerShell script to find and kill processes using a specific port on Windows
# Usage: .\kill_port.ps1 -Port 5000 [-Force]

param(
    [Parameter(Mandatory=$true)]
    [int]$Port,

    [Parameter(Mandatory=$false)]
    [switch]$Force,

    [Parameter(Mandatory=$false)]
    [switch]$Yes
)

Write-Host "Checking port $Port on Windows..." -ForegroundColor Cyan

# Find processes using the port
$connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue

if (-not $connections) {
    Write-Host "✅ Port $Port is free" -ForegroundColor Green
    exit 0
}

# Get unique process IDs
$pids = $connections | Select-Object -ExpandProperty OwningProcess -Unique

Write-Host "`nFound $($pids.Count) process(es) using port ${Port}:" -ForegroundColor Yellow

# Display process information
foreach ($pid in $pids) {
    $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($process) {
        Write-Host "  - $($process.ProcessName) (PID: $pid)" -ForegroundColor White
    } else {
        Write-Host "  - Unknown Process (PID: $pid)" -ForegroundColor White
    }
}

# Ask for confirmation unless -Yes flag is set
if (-not $Yes) {
    $count = $pids.Count
    $plural = if ($count -gt 1) { "es" } else { "" }
    $response = Read-Host "`nKill $(if ($count -gt 1) { "all" } else { "this" }) process$plural? (y/N)"
    if ($response -ne 'y' -and $response -ne 'Y') {
        Write-Host "Cancelled" -ForegroundColor Yellow
        exit 1
    }
}

# Kill processes
$killedCount = 0
foreach ($pid in $pids) {
    $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($process) {
        $processName = $process.ProcessName
        try {
            if ($Force) {
                Stop-Process -Id $pid -Force -ErrorAction Stop
            } else {
                Stop-Process -Id $pid -ErrorAction Stop
            }
            Write-Host "✅ Killed $processName (PID: $pid)" -ForegroundColor Green
            $killedCount++
        } catch {
            Write-Host "❌ Failed to kill $processName (PID: $pid): $_" -ForegroundColor Red
        }
    }
}

# Summary
if ($killedCount -eq $pids.Count) {
    Write-Host "`n✅ Successfully killed all processes on port $Port" -ForegroundColor Green
    exit 0
} elseif ($killedCount -gt 0) {
    Write-Host "`n⚠️  Killed $killedCount/$($pids.Count) processes" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "`n❌ Failed to kill any processes" -ForegroundColor Red
    if (-not $Force) {
        Write-Host "Try using -Force flag" -ForegroundColor Yellow
    }
    exit 1
}
