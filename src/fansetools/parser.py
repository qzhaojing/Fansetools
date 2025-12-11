# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:06:35 2025
v0.2 解析FANSe3结果文件时使用turple存储比对结果，减少内存占用,优化解析速度
v0.1 初始版本，解析FANSe3结果文件，返回FANSeRecord对象
@author: Administrator
"""

import re
import sys  # 新增：使用 sys.intern 对高重复字符串进行驻留，降低内存占用与比较开销
import os
import io
import gzip
import zipfile
import subprocess
import shutil
# import os
from dataclasses import dataclass
from typing import List, Generator
# from typing import List, Generator, Deque
# from collections import deque
# from tqdm import tqdm


@dataclass(slots=True)
class FANSeRecord:
    """
    存储FANSe3单条记录的类
    使用slots减少内存开销
    """
    # 修正：启用 dataclass(slots=True) 以降低每条记录的对象开销，减少内存占用并提升大批处理时的缓存友好性
    
    header: str               # Read名称
    seq: str                  # Read序列
    alignment: str = ''       # 比对结果(可选)
    strands: List[str] = None  # 正负链列表(F/R)
    ref_names: List[str] = None  # 参考序列名称列表
    mismatches: List[int] = None  # 错配数列表
    positions: List[int] = None  # 起始位置列表(0-based)
    multi_count: int = 0      # multi-mapping次数

    def __post_init__(self):
        """初始化后处理，确保列表类型字段不为None"""
        if self.strands is None:
            self.strands = []
        if self.ref_names is None:
            self.ref_names = []
        if self.mismatches is None:
            self.mismatches = []
        if self.positions is None:
            self.positions = []

    def __str__(self):
        """自定义__str__方法，确保ref_names元组转换为逗号分隔的字符串"""
        ref_names_str = ','.join(self.ref_names) if self.ref_names else ''
        return f"FANSeRecord(header='{self.header}', seq='{self.seq}', alignment='{self.alignment}', " \
               f"strands={self.strands}, ref_names='{ref_names_str}', mismatches={self.mismatches}, " \
               f"positions={self.positions}, multi_count={self.multi_count})"

    @property
    def is_multi(self) -> bool:
        """判断是否为多映射记录"""
        return len(self.ref_names) > 1


def _open_fanse_text(file_path: str):
    if file_path.endswith('.gz'):
        pigz = shutil.which('pigz')
        if pigz:
            p = subprocess.Popen([pigz, '-dc', file_path], stdout=subprocess.PIPE)
            return io.TextIOWrapper(p.stdout, encoding='utf-8', errors='ignore')
        return gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore')
    if file_path.endswith('.zip'):
        z = zipfile.ZipFile(file_path)
        names = z.namelist()
        target = names[0] if names else None
        if target is None:
            raise ValueError('Empty zip archive')
        f = z.open(target, 'r')
        return io.TextIOWrapper(f, encoding='utf-8', errors='ignore')
    return open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=1024 * 1024 * 16)

def fanse_line_reader(file_path: str, chunk_size: int = 20000) -> Generator[List[str], None, None]:
    """
    按块读取FANSe3结果文件，返回原始行列表
    
    参数:
        file_path: FANSe3结果文件路径
        chunk_size: 每次读取的行数（必须是偶数，因为每个记录占2行）
        
    返回:
        生成器，每次yield一个字符串列表
    """
    # 确保chunk_size是偶数
    if chunk_size % 2 != 0:
        chunk_size += 1
        
    with _open_fanse_text(file_path) as f:
        chunk = []
        count = 0
        for line in f:
            chunk.append(line)
            count += 1
            if count >= chunk_size:
                yield chunk
                chunk = []
                count = 0
        if chunk:
            yield chunk

def parse_records_from_lines(lines: List[str]) -> Generator[FANSeRecord, None, None]:
    """
    从原始行列表解析FANSeRecord对象
    
    参数:
        lines: 包含FANSe3格式原始行的列表
        
    返回:
        生成器，每次yield一个FANSeRecord对象
    """
    comma_split = re.compile(',').split
    
    # 每次处理2行
    for i in range(0, len(lines), 2):
        if i + 1 >= len(lines):
            break
            
        line1 = lines[i].rstrip()
        line2 = lines[i+1].rstrip()
        
        # 快速分割
        fields1 = line1.split('\t')
        if len(fields1) < 2:
            continue
            
        fields2 = line2.split('\t')  
        if len(fields2) < 5:
            continue
        
        # 缓存所有需要的字段
        header_val = fields1[0]
        seq_val = fields1[1]
        alignment_val = fields1[2] if len(fields1) > 2 else ''
        
        strand_field = fields2[0]
        ref_field = fields2[1]
        mismatch_val = int(fields2[2])
        position_field = fields2[3]
        multi_count = int(fields2[4])
        
        # 根据multi_count分支处理
        if multi_count != 1:
            strands = tuple(comma_split(strand_field))
            ref_names = tuple(sys.intern(name) for name in comma_split(ref_field))
            positions = [int(x) for x in comma_split(position_field)]
            mismatches = [mismatch_val] * len(positions)
        else:
            strands = (strand_field,)
            ref_names = (sys.intern(ref_field),)
            positions = [int(position_field)]
            mismatches = [mismatch_val]
        
        # 处理mismatches长度对齐
        len_mismatches = len(mismatches)
        len_positions = len(positions)            
        if len_positions > len_mismatches:
            mismatches += [mismatch_val] * (len_positions - len_mismatches)
            
        # 延迟处理alignment字段
        alignment_processed = alignment_val.split(',') if alignment_val else ''
        
        record = FANSeRecord(
            header=header_val,
            seq=seq_val,
            alignment=alignment_processed,
            strands=strands, 
            ref_names=ref_names,
            mismatches=mismatches,
            positions=positions,
            multi_count=multi_count
        )
        
        yield record

def fanse_parser(file_path: str) -> Generator[FANSeRecord, None, None]:
    """
    解析FANSe3结果文件的主函数

    参数:
        file_path: FANSe3结果文件路径

    返回:
        生成器，每次yield一个FANSeRecord对象
    """
    # tab_split = re.compile(r'\t+').split
    
    with _open_fanse_text(file_path) as f:
        while True:
            # 读取两行作为一个完整记录
            line1 = f.readline().rstrip()
            line2 = f.readline().rstrip()
            if not line1 or not line2:  # 文件结束
                break

            # 解析第一行(使用严格制表符分割)
            # fields = re.split(r'\t+', line1)
            # 使用更快的字符串分割
            fields1 = line1.split('\t')
            fields2 = line2.split('\t')
            # fields = tab_split(line1)

            if len(fields1) < 2:
                continue  # 跳过无效行而不是抛出异常
                # raise ValueError(f"无效的第一行格式: {line1}")

            # 解析第二行
            if len(fields2) < 5:
                continue  # 跳过无效行而不是抛出异常
                # raise ValueError(f"无效的第二行格式: {line2}")
            
            multi_count = int(fields2[4])
            # 处理可能的多值字段
            if multi_count!=1:
                strands = tuple(fields2[0].split(','))
                # 修正：对 ref_names 应用 sys.intern，驻留高重复的转录本/参考序列ID，减少内存与哈希成本
                ref_names = tuple(sys.intern(name) for name in fields2[1].split(','))
                mismatches = [int(fields2[2])]    #fanse 文件中此字段只有一个而非多个用逗号分割，因此测试注释掉上面行
                positions = [int(x) for x in fields2[3].split(',')]

            else:  # single-mapping reads
                # 单映射reads，直接使用字段值
                strands = (fields2[0],)
                # 修正：对单映射的 ref_names 同样应用 sys.intern，确保与多映射保持一致
                ref_names = (sys.intern(fields2[1]),)
                mismatches = [int(fields2[2])]
                positions = [int(fields2[3])]
            
            # 验证字段一致性并处理可能的长度不一致，这里主要是提供给fanse2sam 使用，没有貌似会报错。

            len_mismatches   =  len(mismatches)
            len_positions   =  len(positions)            
            # max_len = max(len_ref_names, len_strands,
            #                len_mismatches, len_positions)
            mismatches += [int(fields2[2])] * (len_positions - len_mismatches)
            # if len_mismatches < max_len:
            #      mismatches += [int(fields2[2])] * (max_len - len_mismatches)
            # if len_strands < max_len:
            #      strands += [''] * (max_len - len_strands)
            # if len_positions < max_len:
            #      positions += [0] * (max_len - len_positions)

            # 创建记录对象
            record = FANSeRecord(
                                header=fields1[0],
                                seq=fields1[1],
                                alignment=fields1[2].split(',') if len(fields1) > 2 else '',
                                strands=strands,
                                ref_names=ref_names,
                                mismatches=mismatches,
                                positions=positions,
                                multi_count=multi_count
            )

            yield record

def fanse_parser_high_performance(file_path: str) -> Generator[FANSeRecord, None, None]:
    """
    高性能版本 - 最大限度减少重复操作
    """
    # 预编译分割器（小幅度提升）
    comma_split = re.compile(',').split
    
    with _open_fanse_text(file_path) as f:
        batch = []  # 批量处理减少yield开销
        batch_size = 50_000  # 调整批大小以降低生成器切换频率，提升吞吐
        
        while True:
            line1 = f.readline()
            line2 = f.readline()
            if not line1 or not line2:
                # 处理剩余批次
                for record in batch:
                    yield record
                break
            
            # 去除换行符
            line1 = line1.rstrip()
            line2 = line2.rstrip()
            
            # 快速分割并缓存
            fields1 = line1.split('\t')
            if len(fields1) < 2:
                continue
                
            fields2 = line2.split('\t')  
            if len(fields2) < 5:
                continue
            
            # 缓存所有需要的字段, 免得后面来回读取开销大
            header_val = fields1[0]
            seq_val = fields1[1]
            alignment_val = fields1[2] if len(fields1) > 2 else ''
            
            strand_field = fields2[0]
            ref_field = fields2[1]
            mismatch_val = int(fields2[2])  # 提前转换int
            position_field = fields2[3]
            multi_count = int(fields2[4])   # 提前转换int
            
            # 根据multi_count分支处理
            if multi_count != 1:
                strands = tuple(comma_split(strand_field))
                # 修正：高性能解析同样对 ref_names 执行 sys.intern，降低字符串重复与比较开销
                ref_names = tuple(sys.intern(name) for name in comma_split(ref_field))
                positions = [int(x) for x in comma_split(position_field)]
                mismatches = [mismatch_val] * len(positions)
            else:
                strands = (strand_field,)
                # 修正：单映射情形下驻留 ref_names
                ref_names = (sys.intern(ref_field),)
                positions = [int(position_field)]
                mismatches = [mismatch_val]
            
            # 验证字段一致性并处理可能的长度不一致，这里主要是提供给fanse2sam 使用，没有貌似会报错。忘记为啥要有这段了，先留着吧 -20251113
            # len_ref_names   =  len(ref_names)
            # len_strands   =  len(strands)
            len_mismatches   =  len(mismatches)
            len_positions   =  len(positions)            
            # max_len = max(len_ref_names, len_strands,
            #                len_mismatches, len_positions)
            mismatches += [mismatch_val] * (len_positions - len_mismatches)
            # 延迟处理alignment字段
            alignment_processed = alignment_val.split(',') if alignment_val else ''
            
            record = FANSeRecord(
                header=header_val,
                seq=seq_val,
                alignment=alignment_processed,
                strands=strands, 
                ref_names=ref_names,
                mismatches=mismatches,
                positions=positions,
                multi_count=multi_count
            )
            
            batch.append(record)
            
            # 批量处理减少yield开销
            if len(batch) >= batch_size:
                for record in batch:
                    yield record
                batch.clear()
                

@dataclass
class UnmappedRecord:
    """存储未比对reads的类"""
    read_id: str
    sequence: str


def unmapped_parser(file_path: str) -> Generator[UnmappedRecord, None, None]:
    """
    解析未比对reads文件

    参数:
        file_path: 输入文件路径（制表符分隔的read_id和序列）

    返回:
        生成器，每次yield一个UnmappedRecord对象
    """
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:  # 跳过空行
                continue

            parts = line.split('\t')
            if len(parts) < 2:
                raise ValueError(f"Invalid unmapped record format: {line}")

            yield UnmappedRecord(read_id=parts[0], sequence=parts[1])


if __name__ == "__main__":
    # test_parser()
    fanse_file = r'G:\verysync_zhaojing\Python_pakages\fanse2sam\R1_1.fanse3'
    for record in fanse_parser(fanse_file):
        print(f"Header: {record.header}")
        print(f"Sequence: {record.seq[:50]}...")
        print(
            f"Alignment: {record.alignment}..." if record.alignment else "No alignment")
        print(f"Reference Names: {record.ref_names}")
        print(f"Is Multi: {record.is_multi}")
        print(f"Positions: {record.positions}")
        print(f"Mismatches: {record.mismatches}")
        print(f"Strands: {record.strands}")
        print(f"Multi Count: {record.multi_count}")
        print("-" * 50)
        break
