# 修正：Windows 环境一键安装脚本（Miniforge、git、Fansetools）
# 目的：统一远程/本地安装流程，解决路径引号与静默安装问题
# 用法：直接在 PowerShell 中执行该脚本，或由 fansetools 远程调用

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Invoke-FansetoolsInstall {
    param(
        [switch]$InstallConda = $true,
        [switch]$InstallFansetools = $true,
        [string]$PipMirror = "https://pypi.tuna.tsinghua.edu.cn/simple"
    )

    Write-Host "--- Windows 安装开始 ---"

    # 修正：统一使用用户目录，避免 Program Files 路径空格导致 /D 引号问题
    $mfDir = Join-Path $env:USERPROFILE "miniforge3"
    $installerPath = Join-Path $env:TEMP "miniforge_setup.exe"

    if ($InstallConda) {
        if (-not (Test-Path $mfDir)) {
            Write-Host "下载 Miniforge 安装程序..."
            $url = "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Windows-x86_64.exe"
            Invoke-WebRequest -Uri $url -OutFile $installerPath

            Write-Host "静默安装 Miniforge..."
            # 修正：/D 参数必须最后且不带引号（NSIS特性）
            $args = @("/S", "/RegisterPython=1", "/AddToPath=1", "/D=$mfDir")
            $p = Start-Process -FilePath $installerPath -ArgumentList $args -Wait -PassThru
            if ($p.ExitCode -ne 0) { Write-Warning "Miniforge 安装退出码: $($p.ExitCode)" }
            Remove-Item $installerPath -ErrorAction SilentlyContinue

            # 修正：初始化 PowerShell 环境
            $condaExe = Join-Path $mfDir "Scripts\conda.exe"
            if (Test-Path $condaExe) {
                & $condaExe init powershell | Out-Null
                Write-Host "已执行 conda init powershell"
            }
        } else {
            Write-Host "Miniforge 已存在，跳过安装"
        }

        # 安装 mamba（加速）
        $condaExe = Join-Path $mfDir "Scripts\conda.exe"
        if (Test-Path $condaExe) {
            & $condaExe install -n base -y mamba -c conda-forge | Out-Null
            Write-Host "mamba 安装完成"
        }
    }

    if ($InstallFansetools) {
        $mambaExe = Join-Path $mfDir "Scripts\mamba.exe"
        $condaExe = Join-Path $mfDir "Scripts\conda.exe"
        if (Test-Path $mambaExe) {
            & $mambaExe install -n base -y git | Out-Null
        } elseif (Test-Path $condaExe) {
            & $condaExe install -n base -y git | Out-Null
        } else {
            Write-Warning "未检测到 conda/mamba，git 安装可能失败；尝试继续"
        }

        $py = Join-Path $mfDir "python.exe"
        if (-not (Test-Path $py)) { $py = "python" }
        Write-Host "使用 Python: $py"

        # 升级 pip 并安装 Fansetools
        & $py -m pip install -U pip -i $PipMirror | Out-Null
        & $py -m pip install git+https://github.com/qzhaojing/Fansetools.git | Out-Null
        Write-Host "Fansetools 安装完成"
    }

    # 验证安装
    try {
        $py = Join-Path $mfDir "python.exe"
        if (-not (Test-Path $py)) { $py = "python" }
        $ver = & $py -c "import fansetools,sys; sys.stdout.write(getattr(fansetools,'__version__',''))"
        if ($ver) { Write-Host "验证 Fansetools: $ver" } else { Write-Warning "无法验证 Fansetools 安装" }
    } catch { Write-Warning "验证失败: $_" }

    Write-Host "--- Windows 安装结束 ---"
}

# 默认执行（允许被远程追加带参数的调用覆盖）
if ($MyInvocation.InvocationName -eq '.') {
    # 被点源时由调用端控制，不自动执行
} else {
    Invoke-FansetoolsInstall
}

