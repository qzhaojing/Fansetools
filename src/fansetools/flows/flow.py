import os
import json
import urllib.request
from pathlib import Path
from ..install import create_launcher, get_platform


FLOWS_DIR = Path(__file__).parent


def flows_base_dir():
    return FLOWS_DIR


def list_flows():
    flows = []
    for p in FLOWS_DIR.glob('*.json'):
        try:
            data = json.loads(p.read_text(encoding='utf-8'))
            flows.append({
                'name': p.stem,
                'alias': data.get('alias', p.stem),
                'entry': data.get('entry'),
            })
        except Exception:
            pass
    return flows


def install_flow(name, script_or_url, alias=None):
    FLOWS_DIR.mkdir(parents=True, exist_ok=True)
    flow_file = FLOWS_DIR / f'{name}.json'
    # 下载或复制脚本到 flows 目录
    try:
        if script_or_url.startswith(('http://', 'https://')):
            dest_script = FLOWS_DIR / Path(script_or_url).name
            urllib.request.urlretrieve(script_or_url, str(dest_script))
        else:
            src = Path(script_or_url)
            dest_script = FLOWS_DIR / src.name
            if src.exists():
                dest_script.write_bytes(src.read_bytes())
            else:
                raise FileNotFoundError(f'脚本不存在: {script_or_url}')
    except Exception as e:
        print(f'获取脚本失败: {e}')
        return False

    data = {
        'alias': alias or name,
        'entry': str(dest_script),
        'steps': [],
        'requires': [],
    }
    flow_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')

    # 创建启动器到 bin/<platform>/<alias>
    from ..install import get_install_dir
    install_dir = get_install_dir()
    os.makedirs(install_dir, exist_ok=True)
    if create_launcher(install_dir, data['alias'], exec_path=str(dest_script)):
        print(f'✓ 已安装流程: {data["alias"]}')
        return True
    return False


def add_flow_subparser(subparsers):
    flow_parser = subparsers.add_parser(
        'flow',
        help='流程管理',
        description='安装/列出流程（轻量脚本包装为可执行）'
    )
    flow_sub = flow_parser.add_subparsers(dest='flow_cmd')

    flow_install = flow_sub.add_parser('install', help='安装流程')
    flow_install.add_argument('name', help='流程名称')
    flow_install.add_argument('script_or_url', help='脚本路径或URL')
    flow_install.add_argument('--alias', help='流程别名（默认同name）')
    flow_install.set_defaults(func=lambda a: install_flow(a.name, a.script_or_url, a.alias))

    flow_list = flow_sub.add_parser('list', help='列出流程')
    flow_list.set_defaults(func=lambda a: [print(f"{f['alias']}: {f['entry']}") for f in list_flows()])
    return flow_parser
