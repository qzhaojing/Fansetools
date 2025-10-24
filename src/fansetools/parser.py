# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:06:35 2025

@author: Administrator
"""

import re
# import os
from dataclasses import dataclass
from typing import List, Generator
# from typing import List, Generator, Deque
# from collections import deque
# from tqdm import tqdm


@dataclass
class FANSeRecord:
    """存储FANSe3单条记录的类"""
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

    @property
    def is_multi(self) -> bool:
        """判断是否为多映射记录"""
        return len(self.ref_names) > 1


def fanse_parser(file_path: str) -> Generator[FANSeRecord, None, None]:
    """
    解析FANSe3结果文件的主函数

    参数:
        file_path: FANSe3结果文件路径

    返回:
        生成器，每次yield一个FANSeRecord对象
    """
    # tab_split = re.compile(r'\t+').split
    
    with open(file_path, 'r', buffering=1024 * 1024*10 ) as f:  #  1 MB缓冲区
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
                strands = fields2[0].split(',')
                ref_names = fields2[1].split(',')
                # mismatches = list(map(int, fields2[2].split(',')))
                # positions = list(map(int, fields2[3].split(',')))
                # mismatches = [int(x) for x in fields2[2].split(',')]
                mismatches = [int(fields2[2])]    #fanse 文件中此字段只有一个而非多个用逗号分割，因此测试注释掉上面行
                positions = [int(x) for x in fields2[3].split(',')]
                # multi_count = int(fields2[4])
            else:  # single-mapping reads
                # 单映射reads，直接使用字段值
                strands = [fields2[0]]
                ref_names = [fields2[1]]
                mismatches = [int(fields2[2])]
                positions = [int(fields2[3])]
            
            # 验证字段一致性并处理可能的长度不一致，这里主要是提供给fanse2sam 使用，没有貌似会报错。
            # len_ref_names   =  len(ref_names)
            # len_strands   =  len(strands)
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
