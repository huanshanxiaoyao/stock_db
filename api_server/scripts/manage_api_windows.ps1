# Stock Data API Windows管理脚本
# 用于管理API服务器后台作业

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "stop", "status", "restart", "logs", "list")]
    [string]$Action,

    [int]$Port = 5005,
    [string]$Host = "0.0.0.0",
    [switch]$Debug,
    [int]$JobId
)

function Get-ApiJobs {
    return Get-Job | Where-Object { $_.Name -like "*API*" -or $_.Command -like "*start_api_windows.ps1*" -or $_.Command -like "*python*api_server*" }
}

function Show-ApiStatus {
    $apiJobs = Get-ApiJobs

    if ($apiJobs.Count -eq 0) {
        Write-Host "没有运行中的API服务器作业" -ForegroundColor Yellow
        return
    }

    Write-Host "API服务器作业状态:" -ForegroundColor Cyan
    foreach ($job in $apiJobs) {
        $output = Receive-Job -Id $job.Id -Keep | Select-Object -Last 3
        Write-Host "  作业ID: $($job.Id), 状态: $($job.State)" -ForegroundColor White
        if ($output) {
            Write-Host "  最新输出: $($output -join '; ')" -ForegroundColor Gray
        }
    }
}

function Stop-ApiServer {
    param([int]$SpecificJobId = $null)

    if ($SpecificJobId) {
        $job = Get-Job -Id $SpecificJobId -ErrorAction SilentlyContinue
        if ($job) {
            Stop-Job -Id $SpecificJobId
            Remove-Job -Id $SpecificJobId -Force
            Write-Host "已停止作业 $SpecificJobId" -ForegroundColor Green
        } else {
            Write-Error "作业 $SpecificJobId 不存在"
        }
    } else {
        $apiJobs = Get-ApiJobs
        foreach ($job in $apiJobs) {
            Stop-Job -Id $job.Id
            Remove-Job -Id $job.Id -Force
            Write-Host "已停止作业 $($job.Id)" -ForegroundColor Green
        }

        if ($apiJobs.Count -eq 0) {
            Write-Host "没有运行中的API服务器" -ForegroundColor Yellow
        }
    }
}

function Show-ApiLogs {
    param([int]$SpecificJobId = $null)

    if ($SpecificJobId) {
        $job = Get-Job -Id $SpecificJobId -ErrorAction SilentlyContinue
        if ($job) {
            Write-Host "作业 $SpecificJobId 的输出:" -ForegroundColor Cyan
            Receive-Job -Id $SpecificJobId -Keep
        } else {
            Write-Error "作业 $SpecificJobId 不存在"
        }
    } else {
        $apiJobs = Get-ApiJobs
        foreach ($job in $apiJobs) {
            Write-Host "作业 $($job.Id) 的输出:" -ForegroundColor Cyan
            Receive-Job -Id $job.Id -Keep
            Write-Host ("-" * 50) -ForegroundColor Gray
        }
    }
}

function Test-ApiHealth {
    param([int]$TestPort = 5005)

    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$TestPort/health" -TimeoutSec 5 -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            Write-Host "API服务器在端口 $TestPort 正常运行" -ForegroundColor Green
            return $true
        }
    } catch {
        Write-Host "API服务器在端口 $TestPort 无响应" -ForegroundColor Red
        return $false
    }
}

# 主要操作逻辑
switch ($Action) {
    "start" {
        Write-Host "启动API服务器..." -ForegroundColor Green
        $scriptPath = Join-Path $PSScriptRoot "start_api_windows.ps1"

        $params = @{
            Port = $Port
            Host = $Host
        }
        if ($Debug) { $params.Debug = $true }

        & $scriptPath @params
    }

    "stop" {
        Write-Host "停止API服务器..." -ForegroundColor Yellow
        Stop-ApiServer -SpecificJobId $JobId
    }

    "status" {
        Show-ApiStatus
        Test-ApiHealth -TestPort $Port
    }

    "restart" {
        Write-Host "重启API服务器..." -ForegroundColor Yellow
        Stop-ApiServer
        Start-Sleep -Seconds 2

        $scriptPath = Join-Path $PSScriptRoot "start_api_windows.ps1"
        $params = @{
            Port = $Port
            Host = $Host
        }
        if ($Debug) { $params.Debug = $true }

        & $scriptPath @params
    }

    "logs" {
        Show-ApiLogs -SpecificJobId $JobId
    }

    "list" {
        Write-Host "所有PowerShell作业:" -ForegroundColor Cyan
        Get-Job | Format-Table Id, Name, State, Command

        Write-Host "`nAPI相关作业:" -ForegroundColor Cyan
        Get-ApiJobs | Format-Table Id, Name, State, Command
    }
}

Write-Host "`n使用说明:" -ForegroundColor Cyan
Write-Host "  启动: .\manage_api_windows.ps1 -Action start [-Port 5005] [-Debug]" -ForegroundColor White
Write-Host "  停止: .\manage_api_windows.ps1 -Action stop [-JobId ID]" -ForegroundColor White
Write-Host "  状态: .\manage_api_windows.ps1 -Action status" -ForegroundColor White
Write-Host "  日志: .\manage_api_windows.ps1 -Action logs [-JobId ID]" -ForegroundColor White
Write-Host "  重启: .\manage_api_windows.ps1 -Action restart" -ForegroundColor White
Write-Host "  列表: .\manage_api_windows.ps1 -Action list" -ForegroundColor White