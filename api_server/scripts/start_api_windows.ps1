# Stock Data API Windows Startup Script
# Start API server using PowerShell background jobs

param(
    [int]$Port = 5005,
    [string]$HostAddress = "0.0.0.0",
    [switch]$Debug,
    [string]$Config = "config.yaml",
    [switch]$NoReplica
)

# Check if Python is available
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python not found. Please ensure Python is installed and in PATH."
    exit 1
}

# Check config file
if (-not (Test-Path $Config)) {
    Write-Error "Config file $Config does not exist."
    exit 1
}

# Build startup arguments
$args = @()
$args += "--host", $HostAddress
$args += "--port", $Port
$args += "--config", $Config
if ($Debug) {
    $args += "--debug"
}
if ($NoReplica) {
    $args += "--no-replica"
}

Write-Host "Starting Stock Data API Server..." -ForegroundColor Green
Write-Host "  Address: http://$HostAddress`:$Port" -ForegroundColor Yellow
Write-Host "  Config: $Config" -ForegroundColor Yellow
Write-Host "  Debug: $Debug" -ForegroundColor Yellow
Write-Host "  Replica Mode: $(-not $NoReplica)" -ForegroundColor Yellow

# Start background job
$job = Start-Job -ScriptBlock {
    param($ProjectRoot, $Args)
    Set-Location $ProjectRoot
    & python api_server/start.py @Args
} -ArgumentList (Get-Location).Path, $args

Write-Host "API server started as background job, Job ID: $($job.Id)" -ForegroundColor Green
Write-Host ""
Write-Host "Management commands:" -ForegroundColor Cyan
Write-Host "  Check status: Get-Job -Id $($job.Id)" -ForegroundColor White
Write-Host "  View output: Receive-Job -Id $($job.Id) -Keep" -ForegroundColor White
Write-Host "  Stop job: Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor White
Write-Host "  List all jobs: Get-Job" -ForegroundColor White
Write-Host ""

# Wait for server startup
Write-Host "Waiting for server to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

# Test if server is available
try {
    $response = Invoke-WebRequest -Uri "http://localhost:$Port/health" -TimeoutSec 5 -ErrorAction Stop
    if ($response.StatusCode -eq 200) {
        Write-Host "API server started successfully!" -ForegroundColor Green
        Write-Host "  Health check: http://localhost:$Port/health" -ForegroundColor White
        Write-Host "  API docs: http://localhost:$Port/api/v1/info" -ForegroundColor White
    }
} catch {
    Write-Warning "Server may still be starting up, please check manually later"
    Write-Host "  Health check URL: http://localhost:$Port/health" -ForegroundColor White
}

# Return job object for further use
return $job