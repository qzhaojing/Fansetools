# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 11:06:35 2025

@author: Administrator
"""
import re
from dataclasses import dataclass
# from typing import List, Generator
from typing import List, Generator, Deque
from collections import deque


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
    with open(file_path, 'r') as f:
        while True:
            # 读取两行作为一个完整记录
            line1 = f.readline().strip()
            line2 = f.readline().strip()
            if not line1 or not line2:  # 文件结束
                break

            # 解析第一行(使用严格制表符分割)
            fields = re.split(r'\t+', line1)
            if len(fields) < 2:
                raise ValueError(f"无效的第一行格式: {line1}")

            # 解析第二行
            fields2 = re.split(r'\t+', line2)
            if len(fields2) < 5:
                raise ValueError(f"无效的第二行格式: {line2}")

            # 处理可能的多值字段
            strands = fields2[0].split(',')
            ref_names = fields2[1].split(',')
            mismatches = list(map(int, fields2[2].split(',')))
            positions = list(map(int, fields2[3].split(',')))
            multi_count = int(fields2[4])

            # 验证字段一致性并处理可能的长度不一致
            max_len = max(len(ref_names), len(strands),
                          len(mismatches), len(positions))
            if len(strands) < max_len:
                strands += [''] * (max_len - len(strands))
            if len(mismatches) < max_len:
                mismatches += [0] * (max_len - len(mismatches))
            if len(positions) < max_len:
                positions += [0] * (max_len - len(positions))

            # 创建记录对象
            record = FANSeRecord(
                header=fields[0],
                seq=fields[1],
                alignment=fields[2].split(',') if len(fields) > 2 else '',
                strands=strands,
                ref_names=ref_names,
                mismatches=mismatches,
                positions=positions,
                multi_count=multi_count
            )

            yield record

# def fanse_parser(file_path: str, buffer_size: int = 100*1024*1024) -> Generator[FANSeRecord, None, None]:
#     """
#     带缓冲的高性能FANSe3解析器

#     参数:
#         file_path: 输入文件路径
#         buffer_size: 缓冲区大小(字节)

#     返回:
#         生成器，每次yield一个FANSeRecord对象
#     """
#     buffer: Deque[str] = deque()
#     incomplete_line = ""
#     count = 0
#     with open(file_path, 'r') as f:
#         while True:
#             # 填充缓冲区
#             chunk = f.read(buffer_size)
#             if not chunk and not buffer:
#                 break

#             # 处理换行符可能被截断的情况
#             lines = chunk.split('\n')
#             if incomplete_line:
#                 lines[0] = incomplete_line + lines[0]
#                 incomplete_line = ""

#             if chunk and chunk[-1] != '\n':
#                 incomplete_line = lines.pop()
#             buffer.extend(lines)
#             count += 1
#             print("Block {}".format(count), buffer_size)

#             # 处理缓冲区中的记录
#             while len(buffer) >= 2:
#                 line1 = buffer.popleft().strip()
#                 line2 = buffer.popleft().strip()

#                 if not line1 or not line2:
#                     continue

#                 # 解析第一行
#                 fields1 = re.split(r'\t+', line1)
#                 if len(fields1) < 2:
#                     raise ValueError(f"无效的第一行格式: {line1}")

#                 # 解析第二行
#                 fields2 = re.split(r'\t+', line2)
#                 if len(fields2) < 5:
#                     raise ValueError(f"无效的第二行格式: {line2}")

#                 # 处理多值字段
#                 strands = fields2[0].split(',')
#                 ref_names = fields2[1].split(',')
#                 mismatches = list(map(int, fields2[2].split(',')))
#                 positions = list(map(int, fields2[3].split(',')))
#                 multi_count = int(fields2[4])

#                 # 字段对齐
#                 max_len = max(len(ref_names), len(strands),
#                               len(mismatches), len(positions))
#                 strands += [''] * (max_len - len(strands))
#                 mismatches += [0] * (max_len - len(mismatches))
#                 positions += [0] * (max_len - len(positions))

#                 yield FANSeRecord(
#                     header=fields1[0],
#                     seq=fields1[1],
#                     alignment=fields1[2].split(
#                         ',') if len(fields1) > 2 else '',
#                     strands=strands,
#                     ref_names=ref_names,
#                     mismatches=mismatches,
#                     positions=positions,
#                     multi_count=multi_count
#                 )


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
