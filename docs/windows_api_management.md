# Windows PowerShell API服务器管理指南

## 概述

在Windows环境下，我们提供了专门的PowerShell脚本来管理API服务器的后台运行。

## 快速开始

### 1. 启动API服务器

```powershell
# 方法1: 使用管理脚本（推荐）
.\scripts\manage_api_windows.ps1 -Action start

# 方法2: 直接使用启动脚本
.\scripts\start_api_windows.ps1

# 方法3: 指定端口和参数
.\scripts\manage_api_windows.ps1 -Action start -Port 5001 -Debug
```

### 2. 查看服务器状态

```powershell
# 查看API服务器状态
.\scripts\manage_api_windows.ps1 -Action status

# 查看所有后台作业
.\scripts\manage_api_windows.ps1 -Action list
```

### 3. 停止API服务器

```powershell
# 停止所有API服务器
.\scripts\manage_api_windows.ps1 -Action stop

# 停止指定作业ID的服务器
.\scripts\manage_api_windows.ps1 -Action stop -JobId 3
```

### 4. 查看日志

```powershell
# 查看所有API服务器日志
.\scripts\manage_api_windows.ps1 -Action logs

# 查看指定作业的日志
.\scripts\manage_api_windows.ps1 -Action logs -JobId 3
```

## PowerShell作业管理

### 原生PowerShell命令

如果你熟悉PowerShell，也可以直接使用这些命令：

```powershell
# 查看所有后台作业
Get-Job

# 查看特定作业状态
Get-Job -Id 3

# 查看作业输出（不删除）
Receive-Job -Id 3 -Keep

# 查看作业输出（删除已显示的输出）
Receive-Job -Id 3

# 停止作业
Stop-Job -Id 3

# 删除作业
Remove-Job -Id 3

# 停止并删除作业
Stop-Job -Id 3; Remove-Job -Id 3
```

### 手动启动（不使用脚本）

```powershell
# 启动后台作业
$job = Start-Job -ScriptBlock {
    Set-Location "D:\Users\Jack\stock_db"
    python start_api.py --port 5000
}

# 查看作业信息
$job

# 实时查看输出
Receive-Job -Job $job -Keep

# 停止作业
Stop-Job -Job $job
Remove-Job -Job $job
```

## 进程管理（任务管理器方式）

### 查找Python进程

```powershell
# 查找所有Python进程
Get-Process python*

# 查找API服务器进程（包含start_api）
Get-Process | Where-Object { $_.CommandLine -like "*start_api*" }

# 查找占用特定端口的进程
netstat -ano | findstr :5000
```

### 杀死进程

```powershell
# 通过进程ID杀死
Stop-Process -Id 1234 -Force

# 通过进程名杀死所有Python进程（谨慎使用）
Stop-Process -Name python -Force

# 杀死占用端口的进程
$port = 5000
$process = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
if ($process) {
    Stop-Process -Id $process.OwningProcess -Force
}
```

## 常见问题和解决方案

### 1. 端口被占用

```powershell
# 检查端口占用
netstat -ano | findstr :5000

# 杀死占用端口的进程
$port = 5000
$connections = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
foreach ($conn in $connections) {
    Write-Host "杀死进程 $($conn.OwningProcess)"
    Stop-Process -Id $conn.OwningProcess -Force
}
```

### 2. 权限问题

```powershell
# 以管理员身份运行PowerShell
Start-Process powershell -Verb RunAs

# 检查执行策略
Get-ExecutionPolicy

# 设置执行策略（如果需要）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3. 作业丢失或无响应

```powershell
# 清理所有停止的作业
Get-Job | Where-Object {$_.State -eq "Completed" -or $_.State -eq "Failed"} | Remove-Job

# 强制停止所有API相关作业
Get-Job | Where-Object { $_.Command -like "*start_api*" } | Stop-Job
Get-Job | Where-Object { $_.Command -like "*start_api*" } | Remove-Job -Force
```

### 4. 检查服务器是否正常运行

```powershell
# HTTP健康检查
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5000/health" -TimeoutSec 5
    Write-Host "服务器正常: $($response.StatusCode)" -ForegroundColor Green
} catch {
    Write-Host "服务器无响应: $($_.Exception.Message)" -ForegroundColor Red
}

# 检测端口监听
Test-NetConnection -ComputerName localhost -Port 5000
```

## 开机自启动（可选）

如果需要开机自启动API服务器：

### 方法1: 任务计划程序

1. 打开任务计划程序 (`taskschd.msc`)
2. 创建基本任务
3. 设置触发器为"计算机启动时"
4. 操作设置为运行PowerShell脚本

### 方法2: 注册表启动

```powershell
# 添加到注册表启动项
$regPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$scriptPath = "D:\Users\Jack\stock_db\scripts\start_api_windows.ps1"
Set-ItemProperty -Path $regPath -Name "StockDataAPI" -Value "powershell.exe -File '$scriptPath'"

# 删除启动项
Remove-ItemProperty -Path $regPath -Name "StockDataAPI" -ErrorAction SilentlyContinue
```

## 性能监控

```powershell
# 监控API服务器进程
while ($true) {
    $processes = Get-Process python* -ErrorAction SilentlyContinue
    foreach ($proc in $processes) {
        Write-Host "进程: $($proc.Id), CPU: $($proc.CPU), 内存: $([math]::Round($proc.WorkingSet/1MB,2))MB"
    }
    Start-Sleep -Seconds 5
    Clear-Host
}
```

## 故障排查清单

1. **检查Python环境**: `python --version`
2. **检查工作目录**: 确保在项目根目录
3. **检查配置文件**: 确保`config.yaml`存在
4. **检查端口**: 确保端口未被占用
5. **检查权限**: 确保有足够的权限
6. **查看日志**: 检查作业输出和错误信息
7. **重启服务**: 停止所有作业重新启动