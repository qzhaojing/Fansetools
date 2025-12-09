"""Rust 加速解析计数的 Python 适配层

提供以下接口：
- rust_fastcount_available(): 检查本地是否已编译安装 fansetools_fastcount 模块
- parse_and_count_rust(paths): 使用 Rust 引擎一次性解析多个 FANSe3 文件并返回计数结果

返回的计数结构与 FanseCounter.parse_fanse_file_optimized_final 初始化的 isoform 计数器一致：
{ 'raw': {id_or_combo: n}, 'unique_to_isoform': {...}, 'multi_to_isoform': {...}, 'firstID': {...}, 'multi2all': {...} }
"""
from typing import Dict, List

def rust_fastcount_available() -> bool:
    """检测 Rust 扩展是否可导入"""
    try:
        import fansetools_fastcount  # noqa: F401
        return True
    except Exception:
        return False

def parse_and_count_rust(paths: List[str]) -> Dict[str, Dict[str, int]]:
    """调用 Rust 扩展完成解析与计数
    - paths: FANSe3 文件列表（支持 .fanse3 与 .fanse3.gz）
    - 返回：五个 isoform 水平的基础计数器（字典），后续由 Python 层生成 EM/EQ 等衍生计数
    """
    import fansetools_fastcount
    result = fansetools_fastcount.parse_files(paths)
    return result

