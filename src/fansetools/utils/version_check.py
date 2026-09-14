# src/fansetools/utils/version_check.py
"""
版本检测和更新提示模块 - 支持PyPI和GitHub
"""
import os
import re
import sys
import json
import time
import requests
import warnings
import subprocess
from pathlib import Path
from packaging import version
from datetime import datetime, timedelta

def _find_git_root(start):
    """向上查找包含 .git 的目录，找不到返回 None。

    新增(2026-09-14): 安装方式与本地 commit 检测不能依赖 CWD ——
    包的真实源码位置才是 git pull / pip install -e . 的正确执行位置。
    """
    try:
        start = Path(start).resolve()
    except Exception:
        return None
    for p in [start, *start.parents]:
        if (p / '.git').exists():
            return p
    return None


def _get_editable_project_dir():
    """读取 pip show 的 'Editable project location' 字段，非可编辑安装返回 None。

    新增(2026-09-14): 可编辑安装（pip install -e .）的真实源码目录与 CWD 无关；
    git pull 与重新安装必须在该目录执行，否则更新错仓库或直接失败。
    """
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'show', 'fansetools'],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.lower().startswith('editable project location'):
                    loc = line.split(':', 1)[1].strip()
                    if loc:
                        return Path(loc)
    except Exception:
        pass
    return None


class DualVersionChecker:
    """双平台版本检测器（PyPI + GitHub）"""
    
    def __init__(self, current_version, package_name="fansetools", 
                 github_repo="qzhaojing/fansetools",  # 替换为您的GitHub仓库
                 check_interval_days=7, enable_check=True):
        self.current_version = current_version
        self.package_name = package_name
        self.github_repo = github_repo
        self.check_interval_days = check_interval_days
        self.enable_check = enable_check
        self.cache_file = Path.home() / f".{package_name}_version_cache.json"
        
        # API端点
        self.pypi_url = f"https://pypi.org/pypi/{package_name}/json"
        self.github_api_url = f"https://api.github.com/repos/{github_repo}/commits"
        self.github_repo_url = f"https://github.com/{github_repo}"
        
    def should_check_version(self):
        """判断是否应该检查版本"""
        if not self.enable_check:
            return False
            
        if not self.cache_file.exists():
            return True
            
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            last_check = cache_data.get('last_check_time', 0)
            current_time = time.time()
            
            # 检查间隔是否超过设定天数
            return (current_time - last_check) > (self.check_interval_days * 24 * 3600)
        except:
            return True
    
    def get_pypi_latest_version(self):
        """从PyPI获取最新发布版本"""
        try:
            response = requests.get(self.pypi_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data['info']['version']
        except requests.RequestException as e:
            if os.getenv('FANSETOOLS_DEBUG'):
                print(f"PyPI连接失败: {e}")
            return None
        except (KeyError, ValueError) as e:
            if os.getenv('FANSETOOLS_DEBUG'):
                print(f"PyPI版本解析失败: {e}")
            return None
    
    def get_github_latest_commit(self):
        """从GitHub获取最新commit信息"""
        try:
            # 获取默认分支的最新commit
            repo_info_url = f"https://api.github.com/repos/{self.github_repo}"
            repo_response = requests.get(repo_info_url, timeout=10)
            repo_response.raise_for_status()
            repo_data = repo_response.json()
            default_branch = repo_data.get('default_branch', 'main')
            
            # 获取最新commit
            commits_url = f"{self.github_api_url}?sha={default_branch}&per_page=1"
            headers = {}
            if os.getenv('GITHUB_TOKEN'):
                headers['Authorization'] = f"token {os.getenv('GITHUB_TOKEN')}"
                
            response = requests.get(commits_url, headers=headers, timeout=10)
            response.raise_for_status()
            commits_data = response.json()
            
            if commits_data and len(commits_data) > 0:
                latest_commit = commits_data[0]
                return {
                    'sha': latest_commit['sha'][:7],
                    'full_sha': latest_commit['sha'],
                    'message': latest_commit['commit']['message'].split('\n')[0],
                    'date': latest_commit['commit']['committer']['date'],
                    'url': latest_commit['html_url'],
                    'author': latest_commit['commit']['author']['name']
                }
        except requests.RequestException as e:
            if os.getenv('FANSETOOLS_DEBUG'):
                print(f"GitHub API连接失败: {e}")
        except (KeyError, ValueError) as e:
            if os.getenv('FANSETOOLS_DEBUG'):
                print(f"GitHub数据解析失败: {e}")
        
        return None
    
    def get_local_commit_info(self):
        """获取本地Git信息（如果是从Git安装）"""
        # 修正(2026-09-14): 原实现只检测 CWD 是否为 git 仓库 —— CWD 与包安装
        # 位置无关，用户在数据目录运行 fanse 时会误判 is_git_install=False，
        # 连带 github_update_available 永远为 False、perform_update 误报
        # "已是最新版本"。现优先从本模块所在包目录向上找 .git（可编辑安装
        # 即源码仓库），再回退 CWD。
        candidates = [Path(__file__).resolve().parent]
        try:
            candidates.append(Path.cwd())
        except Exception:
            pass
        for cand in candidates:
            git_root = _find_git_root(cand)
            if git_root is None:
                continue
            try:
                commit_result = subprocess.run([
                    'git', 'rev-parse', 'HEAD'
                ], capture_output=True, text=True, timeout=5, cwd=str(git_root))

                if commit_result.returncode == 0:
                    current_sha = commit_result.stdout.strip()[:7]

                    # 获取当前分支
                    branch_result = subprocess.run([
                        'git', 'branch', '--show-current'
                    ], capture_output=True, text=True, timeout=5, cwd=str(git_root))

                    branch = branch_result.stdout.strip() if branch_result.returncode == 0 else "unknown"

                    return {
                        'sha': current_sha,
                        'branch': branch,
                        'is_git_install': True
                    }
            except Exception:
                continue

        return {'is_git_install': False}
    
    def check_version(self):
        """检查两个平台的版本信息"""
        if not self.should_check_version():
            return None
            
        pypi_latest = self.get_pypi_latest_version()
        github_latest = self.get_github_latest_commit()
        local_git_info = self.get_local_commit_info()
        
        # 更新缓存
        self._update_cache()
        
        result = {
            'current_version': self.current_version,
            'pypi_latest': pypi_latest,
            'github_latest': github_latest,
            'local_git_info': local_git_info,
            'check_time': datetime.now().isoformat()
        }
        
        # 检查PyPI更新
        if pypi_latest:
            current_ver = version.parse(self.current_version)
            pypi_ver = version.parse(pypi_latest)
            
            result['pypi_update_available'] = pypi_ver > current_ver
            result['is_major_update'] = pypi_ver.major > current_ver.major
            result['is_minor_update'] = (pypi_ver.major == current_ver.major and 
                                       pypi_ver.minor > current_ver.minor)
        else:
            result['pypi_update_available'] = False
        
        # 检查GitHub更新（针对Git安装）
        if github_latest and local_git_info.get('is_git_install'):
            # 这里可以添加更复杂的Git更新检测逻辑
            result['github_update_available'] = True  # 简化处理
        else:
            result['github_update_available'] = False
        
        result['any_update_available'] = (
            result['pypi_update_available'] or 
            result['github_update_available']
        )
        
        return result
    
    def _update_cache(self):
        """更新缓存文件"""
        cache_data = {
            'last_check_time': time.time(),
            'last_check_version': self.current_version
        }
        
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f)
        except:
            pass
    
    def format_update_message(self, version_info):
        """格式化更新提示信息"""
        if not version_info or not version_info.get('any_update_available'):
            return None
        
        messages = []
        messages.append("=" * 70)
        messages.append("🎉 发现更新!")
        messages.append(f"当前版本: {version_info['current_version']}")
        messages.append("")
        
        # PyPI更新信息
        if version_info.get('pypi_update_available'):
            messages.append("📦 PyPI发布版本更新:")
            messages.append(f"  当前: {version_info['current_version']} → 最新: {version_info['pypi_latest']}")
            
            if version_info.get('is_major_update'):
                messages.append("  ⚠️  主要版本更新，建议尽快升级！")
            elif version_info.get('is_minor_update'):
                messages.append("  ✨ 次要版本更新，包含新功能和改进")
            else:
                messages.append("  🔧 补丁版本更新，修复了已知问题")
            
            messages.append("")
            messages.append("  升级命令:")
            messages.append(f"    pip install --upgrade {self.package_name}")
            messages.append("")
            messages.append("  或者使用conda升级:")
            messages.append(f"    conda update {self.package_name}")
            messages.append("")
        
        # GitHub commit更新信息
        if version_info.get('github_update_available') and version_info.get('github_latest'):
            gh_info = version_info['github_latest']
            local_info = version_info['local_git_info']
            
            messages.append("🐙 GitHub代码更新:")
            messages.append(f"  仓库: {self.github_repo}")
            
            if local_info.get('is_git_install'):
                messages.append(f"  当前分支: {local_info.get('branch', 'unknown')}")
                messages.append(f"  最新提交: {gh_info['sha']} - {gh_info['message']}")
                messages.append(f"  作者: {gh_info['author']}")
                messages.append(f"  时间: {gh_info['date'][:10]}")
                messages.append("")
                messages.append("  更新命令:")
                messages.append("    git pull origin main")
                messages.append("    pip install -e .  # 重新安装开发版本")
            else:
                messages.append("  最新提交信息:")
                messages.append(f"    {gh_info['sha']} - {gh_info['message']}")
                messages.append(f"    {gh_info['author']} - {gh_info['date'][:10]}")
                messages.append("")
                messages.append("  查看详情:")
                messages.append(f"    {gh_info['url']}")
            
            messages.append("")
        
        # 通用信息
        messages.append("相关链接:")
        messages.append(f"  PyPI页面: https://pypi.org/project/{self.package_name}/")
        messages.append(f"  GitHub仓库: {self.github_repo_url}")
        messages.append(f"  更新日志: {self.github_repo_url}/releases")
        
        messages.append("=" * 70)
        messages.append("")
        
        return "\n".join(messages)
    
    def show_update_notification(self, force_check=False):
        """显示更新通知"""
        if force_check or self.should_check_version():
            version_info = self.check_version()
            if version_info and version_info.get('any_update_available'):
                message = self.format_update_message(version_info)
                if message:
                    print(message)
                    return True
        return False


    def perform_update(self, interactive=True):
        """执行更新操作"""
        installation_method = get_installation_method()
        print("检查更新中...")
        version_info = self.check_version()

        update_available = bool(version_info and version_info.get('any_update_available'))
        # 新增(2026-09-14): 显式更新意图放行。
        # 意图：install_win.ps1 / install_linux.sh 安装的机器
        # （pip install git+https://...）运行目录通常不是 git 仓库，且
        # fansetools 未发布 PyPI，原逻辑此时直接报"已是最新版本"返回、
        # 从不执行升级。用户运行 'fanse update' 即表达升级意图 →
        # pip/git 安装一律放行，由 pip --force-reinstall 拉取仓库最新
        # commit（版本号不变也会重装）；unknown 安装方式才维持旧行为。
        force_sync = installation_method in ('pip', 'git')

        if not update_available and not force_sync:
            print(f"当前已是最新版本：{self.current_version}，无需更新。")
            return True

        print("=" * 60)
        print("开始更新 fansetools")
        print("=" * 60)

        if interactive:
            # 显示更新信息
            if update_available:
                if version_info.get('pypi_update_available'):
                    print(f"发现新版本: {version_info['pypi_latest']} (当前: {version_info['current_version']})")
                elif version_info.get('github_update_available'):
                    print("发现GitHub代码更新")
            else:
                print("将同步 GitHub 仓库最新代码（git 安装的版本号可能不随 commit 变化）")

            # 确认更新
            try:
                response = input("是否立即更新? [y/N]: ").strip().lower()
                if response not in ['y', 'yes']:
                    print("更新已取消。")
                    return False
            except KeyboardInterrupt:
                print("\n更新已取消。")
                return False

        try:
            if installation_method == 'pip':
                print("使用pip进行更新（git+https 拉取仓库最新 commit）...")
                # 修正(2026-09-14): 原实现把 '--force-reinstall' 与 URL 拼成
                # 单个参数 'git+https://... --force-reinstall' 传给 pip，
                # pip 将其视为 URL 一部分直接报错 —— 这是 'fanse update'
                # 长期失效的直接原因。现拆分为独立参数；同时不再
                # capture_output，让 pip 的下载/安装进度实时可见。
                subprocess.run([
                    sys.executable, '-m', 'pip', 'install',
                    '--upgrade', '--force-reinstall',
                    f'git+https://github.com/{self.github_repo}.git',
                ], check=True)
                new_ver = None
                try:
                    out = subprocess.run([sys.executable, '-c', 'import fansetools; print(getattr(fansetools, "__version__", "unknown"))'], capture_output=True, text=True)
                    if out.returncode == 0:
                        new_ver = out.stdout.strip()
                except Exception:
                    pass
                print(f"更新完毕，已同步至仓库最新提交（版本号: {new_ver or '未知'}）")
                return True

            elif installation_method == 'conda':
                print("使用conda进行更新...")
                result = subprocess.run([
                    'conda', 'update', self.package_name
                ], check=True, capture_output=True, text=True)
                print(f"更新完毕，最新版本 {version_info.get('pypi_latest') or '未知'}")
                return True

            elif installation_method == 'git':
                print("使用git进行更新...")
                # 修正(2026-09-14): git pull 必须在包源码目录执行。
                # 原实现在 CWD 执行 pull —— 用户在其他目录运行 fanse update
                # 时要么 pull 错误的仓库，要么直接失败。现优先用 pip show 的
                # Editable project location，回退到本模块所在的 git 根目录。
                src_dir = _get_editable_project_dir()
                if src_dir is None:
                    src_dir = _find_git_root(Path(__file__).resolve().parent)
                if src_dir is None:
                    print("无法定位源码目录，请手动更新:")
                    print("  cd <Fansetools源码目录>")
                    print("  git pull origin main")
                    print(f"  {sys.executable} -m pip install -e .")
                    return False
                print(f"源码目录: {src_dir}")
                subprocess.run(['git', 'pull'], check=True, cwd=str(src_dir))
                # 重新安装
                subprocess.run([
                    sys.executable, '-m', 'pip', 'install', '-e', str(src_dir)
                ], check=True)
                print("更新完毕，已同步至仓库最新提交")
                return True

            else:
                print("无法确定安装方式，请手动更新:")
                if version_info.get('pypi_latest'):
                    print(f"  pip install --upgrade {self.package_name}")
                return False
                
        except subprocess.CalledProcessError as e:
            print(f"更新失败: {e}")
            if e.stderr:
                print(f"错误信息: {e.stderr}")
            return False
        except Exception as e:
            print(f"更新过程中发生错误: {e}")
            return False
            
def get_installation_method():
    """检测安装方式

    修正(2026-09-14): 原实现有两个误判源：
      1) 'pip show fansetools' 成功即判为 pip —— 但 pip show 对
         pip install -e .（可编辑）与 pip install git+https 安装同样返回 0，
         导致可编辑安装走错分支（会被 git+https 重装覆盖）；
      2) 'git rev-parse' 在 CWD 检测 —— CWD 与包安装位置无关，
         用户在数据目录运行 fanse update 会把任意 git 仓库误判为 git 安装。
    现改为：pip show 优先（覆盖 git+https 与可编辑安装）；可编辑安装再查
    源码目录是否含 .git 决定 git/pip 分支；pip 检测失败才回退 conda；
    最后以包自身位置向上找 .git 兜底（源码直跑、未注册 pip 的场景）。
    """
    # 1) pip 系安装（含 pip install git+https://... 与 pip install -e .）
    try:
        result = subprocess.run([
            sys.executable, '-m', 'pip', 'show', 'fansetools'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            editable_dir = _get_editable_project_dir()
            if editable_dir and (editable_dir / '.git').exists():
                return 'git'   # 可编辑安装 + 源码目录是 git 仓库 → git pull 模式
            return 'pip'       # git+https / 普通安装 → pip 重装模式
    except Exception:
        pass

    # 2) conda 安装（pip show 失败时才可能走到）
    try:
        result = subprocess.run([
            'conda', 'list', 'fansetools'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0 and 'fansetools' in result.stdout:
            return 'conda'
    except Exception:
        pass

    # 3) 兜底：包自身位于 git 工作树内（源码直跑、未注册 pip）
    git_root = _find_git_root(Path(__file__).resolve().parent)
    if git_root is not None:
        return 'git'

    return 'unknown'


def check_fansetools_version():
    """检查fansetools版本的主函数"""
    try:
        from .. import __version__
        
        # 用户可通过环境变量禁用版本检查
        if os.getenv('FANSETOOLS_DISABLE_VERSION_CHECK'):
            return
        
        # 根据安装方式决定GitHub仓库（替换为您的实际仓库）
        github_repo = "qzhaojing/fansetools"  # 请修改为您的GitHub用户名和仓库名
        
        checker = DualVersionChecker(
            current_version=__version__,
            package_name="fansetools",
            github_repo=github_repo,
            check_interval_days=1,
            enable_check=True
        )
        
        # 只在交互式终端中显示通知
        if sys.stdout.isatty():
            checker.show_update_notification()
            
    except ImportError:
        # 如果无法导入版本信息，跳过检查
        pass
    except Exception as e:
        # 版本检查不应该影响主要功能
        if os.getenv('FANSETOOLS_DEBUG'):
            print(f"版本检查错误: {e}")
            
            
def update_fansetools(args):
    """更新fansetools的主函数"""
    try:
        from .. import __version__, __github_repo__
        
        checker = DualVersionChecker(
            current_version=__version__,
            package_name="fansetools",
            github_repo=__github_repo__,
            check_interval_days=0,  # 强制检查
            enable_check=True
        )
        
        return checker.perform_update(interactive=not args.yes)
        
    except ImportError:
        print("错误: 无法导入fansetools版本信息")
        return False
    except Exception as e:
        print(f"更新过程中发生错误: {e}")
        return False


            
            
            
