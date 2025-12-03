#!/usr/bin/env bash
# 修正：Linux 环境一键安装脚本（Miniforge、git、Fansetools）
# 目的：统一远程/本地安装流程，支持可选镜像，提高安装稳定性

set -euo pipefail

fansetools_install() {
  local install_conda="true"
  local install_fansetools="true"
  local pip_mirror="https://pypi.tuna.tsinghua.edu.cn/simple"

  # 参数解析（简单实现）
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --conda)
        install_conda="$2"; shift 2;;
      --fansetools)
        install_fansetools="$2"; shift 2;;
      --pip-mirror)
        pip_mirror="$2"; shift 2;;
      *)
        echo "未知参数: $1"; shift;;
    esac
  done

  echo "--- Linux 安装开始 ---"

  local mf_dir="$HOME/miniforge3"
  if [[ "$install_conda" == "true" ]]; then
    if [[ ! -d "$mf_dir" ]]; then
      echo "下载 Miniforge..."
      local url="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
      local installer="$HOME/miniforge_setup.sh"
      wget -q "$url" -O "$installer"
      echo "静默安装 Miniforge..."
      bash "$installer" -b -p "$mf_dir"
      rm -f "$installer"
      "$mf_dir/bin/conda" init bash || true
      echo "Miniforge 安装完成"
    else
      echo "Miniforge 已存在，跳过安装"
    fi
    "$mf_dir/bin/conda" install -n base -y mamba -c conda-forge || true
  fi

  if [[ "$install_fansetools" == "true" ]]; then
    if [[ -x "$mf_dir/bin/mamba" ]]; then
      "$mf_dir/bin/mamba" install -n base -y git || "$mf_dir/bin/conda" install -n base -y git || true
    else
      "$mf_dir/bin/conda" install -n base -y git || true
    fi
    # 使用 miniforge 的 python
    local py="$mf_dir/bin/python"
    if [[ ! -x "$py" ]]; then py="python3"; fi
    "$py" -m pip install -U pip -i "$pip_mirror"
    "$py" -m pip install git+https://github.com/qzhaojing/Fansetools.git
    echo "Fansetools 安装完成"
  fi

  # 验证安装
  local py="$mf_dir/bin/python"; [[ -x "$py" ]] || py="python3"
  local ver
  ver=$("$py" -c "import fansetools,sys; print(getattr(fansetools,'__version__',''))") || true
  if [[ -n "$ver" ]]; then echo "验证 Fansetools: $ver"; else echo "无法验证 Fansetools 安装"; fi

  echo "--- Linux 安装结束 ---"
}

# 允许直接执行脚本进行默认安装
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  fansetools_install "$@"
fi

