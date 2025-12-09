# fansetools/utils/path_utils.py
import os
import glob
import logging
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple
from collections import OrderedDict

class PathProcessor:
    """统一的路径处理器 - 基于run.py的路径处理逻辑重构"""
    
    # 支持的fastq文件扩展名
    FASTQ_EXTENSIONS = ['.fastq', '.fq', '.fastq.gz', '.fq.gz', '.fqc']
    # 支持的fanse文件扩展名  
    FANSE_EXTENSIONS = ['.fanse3', '.fanse3.gz', '.fanse3.zip']
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
    
    def _normalize_path(self, path: Union[str, Path]) -> Path:
        """规范化路径处理，支持UNC和所有Windows路径"""
        if isinstance(path, str):
            path = path.strip().strip('"').strip("'")
            
        path = Path(path)
        
        # 处理网络路径（UNC）的特殊情况
        path_str = str(path)
        if path_str.startswith(('\\\\', '//')):
            unc_path = path_str.replace('/', '\\')
            return Path(unc_path)
        
        try:
            return path.resolve()
        except:
            try:
                return path.absolute()
            except:
                return path
    
    def parse_input_paths(self, input_str: str, valid_extensions: List[str] = None) -> List[Path]:
        """
        解析输入路径字符串，支持多种格式
        
        Args:
            input_str: 输入路径字符串（支持逗号分隔的多个路径）
            valid_extensions: 有效的文件扩展名列表（None表示接受所有文件）
            
        Returns:
            解析后的路径列表
        """
        if not input_str:
            return []
            
        input_items = [item.strip() for item in input_str.split(',') if item.strip()]
        input_paths = []
        
        for item in input_items:
            # 移除可能包裹在路径两端的引号
            item = item.strip('\'"')
            
            # Windows下统一路径分隔符，这对UNC路径的glob匹配至关重要
            if os.name == 'nt':
                item = item.replace('/', '\\')
            
            try:
                # 处理通配符
                if '*' in item or '?' in item:
                    matched_paths = glob.glob(item)
                    if not matched_paths:
                        self.logger.warning(f"未找到匹配的文件: {item}") if self.logger else None
                        continue
                    for mp in matched_paths:
                        p = self._normalize_path(mp)
                        if p.exists():
                            if p.is_file():
                                if self._is_valid_extension(p, valid_extensions):
                                    input_paths.append(p)
                            elif p.is_dir():
                                # 目录：添加目录下所有有效文件
                                self._add_directory_files(p, input_paths, valid_extensions)
                        else:
                            self.logger.warning(f"路径不存在: {mp}") if self.logger else None
                else:
                    # 没有通配符，直接处理路径
                    p = self._normalize_path(item)
                    if p.exists():
                        if p.is_file():
                            if self._is_valid_extension(p, valid_extensions):
                                input_paths.append(p)
                        elif p.is_dir():
                            # 目录：添加目录下所有有效文件
                            self._add_directory_files(p, input_paths, valid_extensions)
                    else:
                        self.logger.warning(f"路径不存在: {item}") if self.logger else None
            except Exception as e:
                error_msg = f"解析输入路径失败: {item} - {str(e)}"
                if self.logger:
                    self.logger.error(error_msg)
                else:
                    print(f"错误: {error_msg}")
        
        # 去重并保持顺序
        return list(OrderedDict.fromkeys(input_paths))
    
    def _is_valid_extension(self, path: Path, valid_extensions: List[str]) -> bool:
        """检查文件扩展名是否有效"""
        if valid_extensions is None:
            return True
            
        # 使用 endswith 检查，支持任意复杂的后缀（如 .counts_gene_level_unique.csv）
        # 这种方式比 pathlib.suffixes 更灵活，且保持向下兼容
        name_lower = path.name.lower()
        return any(name_lower.endswith(ext.lower()) for ext in valid_extensions)
    
    def _add_directory_files(self, directory: Path, file_list: List[Path], valid_extensions: List[str]):
        """将目录下的有效文件添加到文件列表"""
        for file_path in directory.iterdir():
            if file_path.is_file() and self._is_valid_extension(file_path, valid_extensions):
                file_list.append(file_path)
    
    def generate_output_mapping(self, input_paths: List[Path], 
                               output_path: Optional[Union[str, Path]] = None,
                               default_suffix: str = ".output") -> Dict[Path, Path]:
        """
        生成输入输出路径映射
        
        Args:
            input_paths: 输入路径列表
            output_path: 输出路径（文件、目录或None）
            default_suffix: 默认输出文件后缀
            
        Returns:
            输入路径到输出路径的映射字典
        """
        if not input_paths:
            return {}
            
        path_map = OrderedDict()
        
        # 情况1: 没有指定输出路径
        if output_path is None:
            for input_path in input_paths:
                output_file = input_path.with_suffix(default_suffix)
                path_map[input_path] = output_file
        
        # 情况2: 输出路径是单个文件（只支持单个输入）
        elif len(input_paths) == 1 and not Path(output_path).is_dir():
            output_file = self._normalize_path(output_path)
            # 确保父目录存在
            output_file.parent.mkdir(parents=True, exist_ok=True)
            path_map[input_paths[0]] = output_file
        
        # 情况3: 输出路径是目录
        else:
            output_dir = self._normalize_path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for input_path in input_paths:
                # 智能生成输出文件名
                output_filename = self._generate_output_filename(input_path, default_suffix)
                output_file = output_dir / output_filename
                path_map[input_path] = output_file
        
        return path_map
    
    def _generate_output_filename(self, input_path: Path, default_suffix: str) -> str:
        """智能生成输出文件名，处理压缩文件扩展名"""
        stem = input_path.stem
        
        # 处理常见的压缩文件扩展名
        compress_exts = ['.gz', '.bz2', '.zip']
        for ext in compress_exts:
            if stem.endswith(ext):
                stem = stem[:-len(ext)]
                break
        
        return f"{stem}{default_suffix}"
    
    def validate_paths(self, *path_checks: Tuple[Path, str, Dict]) -> Tuple[bool, List[str]]:
        """
        集中验证路径
        
        Args:
            path_checks: 元组列表，每个元组为 (路径, 描述, 检查选项)
                        检查选项: {'must_exist': True, 'must_be_file': True, 'must_be_dir': True}
        
        Returns:
            (是否全部有效, 错误信息列表)
        """
        all_errors = []
        
        for path, description, checks in path_checks:
            errors = self._validate_single_path(path, description, checks)
            all_errors.extend(errors)
        
        return len(all_errors) == 0, all_errors
    
    def _validate_single_path(self, path: Path, description: str, checks: Dict) -> List[str]:
        """验证单个路径"""
        errors = []
        
        # 存在性检查
        if checks.get('must_exist', False) and not path.exists():
            errors.append(f"{description}不存在: {path}")
            return errors  # 如果不存在，其他检查无意义
        
        if not path.exists():
            return errors  # 路径不存在但不是必须存在，直接返回
            
        # 类型检查
        if checks.get('must_be_file', False) and not path.is_file():
            errors.append(f"{description}不是文件: {path}")
            
        if checks.get('must_be_dir', False) and not path.is_dir():
            errors.append(f"{description}不是目录: {path}")
        
        # 可访问性检查（针对目录）
        if checks.get('must_be_writable', False) and path.is_dir():
            test_file = path / ".fanse_write_test.tmp"
            try:
                test_file.touch()
                test_file.unlink()
            except PermissionError:
                errors.append(f"{description}目录不可写: {path}")
        
        # 路径长度检查（Windows限制）
        path_str = str(path.resolve())
        if len(path_str) > 200:  # 预警阈值
            errors.append(f"{description}路径过长（{len(path_str)}字符）: {path}")
        
        return errors

# 便捷函数
def validate_path(path: Path, must_exist: bool = True, must_be_file: bool = False, 
                 must_be_dir: bool = False, must_be_writable: bool = False) -> Tuple[bool, List[str]]:
    """便捷函数：验证单个路径"""
    processor = PathProcessor()
    checks = {
        'must_exist': must_exist,
        'must_be_file': must_be_file, 
        'must_be_dir': must_be_dir,
        'must_be_writable': must_be_writable
    }
    is_valid, errors = processor.validate_paths((path, "路径", checks))
    return is_valid, errors

def generate_output_mapping(input_paths: List[Path], output_path: Optional[str], 
                          default_suffix: str) -> Dict[Path, Path]:
    """便捷函数：快速生成路径映射"""
    processor = PathProcessor()
    return processor.generate_output_mapping(input_paths, output_path, default_suffix)