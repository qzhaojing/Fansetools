#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from .utils.path_utils import PathProcessor
from .utils.rich_help import CustomHelpFormatter
from rich.console import Console
from rich.panel import Panel

import pandas as pd
import math
from typing import Any

try:
    from .gxf2refflat_plus import load_refflat_to_dataframe, load_annotation_to_dataframe
except Exception:
    load_refflat_to_dataframe = None
    load_annotation_to_dataframe = None


# 修正：统一构建长度映射，支持 isoform 与 gene 两个层级
def build_length_maps(annotation_df: pd.DataFrame, 
    level: str = 'gene', 
    mode: Optional[str] = None) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    根据注释数据构建长度映射（length_map）和有效长度映射（eff_length_map）。
    - level='gene': 以 `geneName` 为键，优先使用 `genelongesttxLength`，回退 `txLength`(max 聚合)，有效长度优先 `geneEffectiveLength`，否则回退总长度。
    - level='isoform': 以 `txname` 为键，优先使用 `txLength`，有效长度无专用列时回退总长度。
    """
    if annotation_df is None or annotation_df.empty:
        return {}, {}

    # 安全转换函数
    def _safe_float(x, default=0.0):
        try:
            return float(x)
        except (ValueError, TypeError):
            return default

    if level == 'isoform':
        id_col = 'txname' if 'txname' in annotation_df.columns else None
        if not id_col:
            return {}, {}
        # 修正：优先 mode，其次 txLength；若有效长度为0/缺失则回退到总长度，避免TPM为0
        mode_iso = mode or 'txLength'
        candidates = [mode_iso, 'txLength', 'isoformEffectiveLength', 'cdsLength']
        len_col = next((c for c in candidates if c in annotation_df.columns), None)
        if not len_col:
            return {}, {}
        eff_col = 'isoformEffectiveLength' if 'isoformEffectiveLength' in annotation_df.columns else None
        length_map = {k: _safe_float(v) for k, v in zip(annotation_df[id_col], annotation_df[len_col])}
        if eff_col:
            eff_length_map_raw = {k: _safe_float(v) for k, v in zip(annotation_df[id_col], annotation_df[eff_col])}
            # 将无效(<=0或NA)的有效长度替换为总长度
            eff_length_map = {k: (v if (v > 0) else float(length_map.get(k, 0.0))) for k, v in eff_length_map_raw.items()}
        else:
            eff_length_map = length_map
        return length_map, eff_length_map

    # gene level
    if 'geneName' not in annotation_df.columns:
        return {}, {}
    # gene 层长度选择
    mode_gene = mode or 'genelongesttxLength'
    if mode_gene == 'geneEffectiveLength' and 'geneEffectiveLength' in annotation_df.columns:
        length_map = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['geneEffectiveLength'].max().items()}
    elif mode_gene == 'genelongestcdsLength' and 'genelongestcdsLength' in annotation_df.columns:
        length_map = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['genelongestcdsLength'].max().items()}
    elif mode_gene == 'txLength' and 'txLength' in annotation_df.columns:
        length_map = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['txLength'].max().items()}
    elif 'genelongesttxLength' in annotation_df.columns:
        length_map = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['genelongesttxLength'].max().items()}
    elif 'txLength' in annotation_df.columns:
        length_map = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['txLength'].max().items()}
    else:
        length_map = {}

    # 修正：若存在 geneEffectiveLength，则使用它；但对<=0/NA值回退到总长度，避免TPM为0
    if 'geneEffectiveLength' in annotation_df.columns:
        eff_raw = {k: _safe_float(v) for k, v in annotation_df.groupby('geneName')['geneEffectiveLength'].max().items()}
        eff_length_map = {k: (v if (v > 0) else float(length_map.get(k, 0.0))) for k, v in eff_raw.items()}
    else:
        eff_length_map = length_map
    return length_map, eff_length_map


def _compute_tpm_series(counts: pd.Series, eff_len_map: Dict[str, float]) -> pd.Series:
    """
    计算 TPM：TPM = RPK / sum(RPK) * 1e6，其中 RPK = count / (effective_length_kb)
    counts 索引为 ID（geneName 或 txname），值为计数。
    """
    ids = counts.index.tolist()
    eff_kb = pd.Series({i: (eff_len_map.get(i, 0.0) or 0.0) / 1000.0 for i in ids}, dtype='float64')
    rpk = counts.astype('float64').div(eff_kb.replace(0.0, math.nan)).fillna(0.0)
    total_rpk = float(rpk.sum())
    if total_rpk <= 0:
        return pd.Series({i: 0.0 for i in ids})
    scale = 1e6 / total_rpk
    return rpk * scale


def _compute_rpkm_series(counts: pd.Series, len_map: Dict[str, float]) -> pd.Series:
    """
    计算 RPKM：RPKM = count / (length_kb) / (total_counts_millions)
    """
    ids = counts.index.tolist()
    length_kb = pd.Series({i: (len_map.get(i, 0.0) or 0.0) / 1000.0 for i in ids}, dtype='float64')
    total_counts = float(counts.sum())
    denom_millions = total_counts / 1e6 if total_counts > 0 else 0.0
    rpkm = counts.astype('float64').div(length_kb.replace(0.0, math.nan)).fillna(0.0)
    if denom_millions > 0:
        rpkm = rpkm.div(denom_millions)
    else:
        rpkm = pd.Series({i: 0.0 for i in ids})
    return rpkm


# 修正：在唯一文件数据框中追加 TPM/RPKM 列
def add_quant_columns(df: pd.DataFrame,
                      id_col: str,
                      count_cols: List[str],
                      length_map: Dict[str, float],
                      eff_length_map: Dict[str, float],
                      methods: str = 'tpm') -> pd.DataFrame:
    """
    为 `df` 中的指定计数列追加表达量列。
    - `id_col`: 标识列名（isoform: Transcript/txname；gene: Gene）
    - `count_cols`: 需要定量的计数列名列表（例如 Final_EM, Final_EQ, firstID 等）
    - `length_map`: 总长度映射（RPKM 用）
    - `eff_length_map`: 有效长度映射（TPM 用）
    - `methods`: 'tpm' | 'rpkm' | 'both'
    返回追加了新列的数据框，列名规则：TPM_<col> / RPKM_<col>
    """
    if df is None or df.empty or id_col not in df.columns:
        return df
    if not count_cols:
        return df

    # 构造 index -> id 的映射，避免多次查找
    ids = df[id_col].astype(str)

    # 针对每一项计数列分别计算表达量
    for col in count_cols:
        if col not in df.columns:
            continue
        counts = pd.Series(df[col].values, index=ids.values, dtype='float64')

        if methods in ('tpm', 'both'):
            tpm = _compute_tpm_series(counts, eff_length_map)
            df[f'TPM_{col}'] = ids.map(lambda i: float(tpm.get(str(i), 0.0)))

        if methods in ('rpkm', 'both'):
            rpkm = _compute_rpkm_series(counts, length_map)
            df[f'RPKM_{col}'] = ids.map(lambda i: float(rpkm.get(str(i), 0.0)))

    return df


# ========== 独立模块 CLI：汇总矩阵 + 导出常见格式 ==========

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _sample_id_from_path(p: str, suffix: str) -> str:
    stem = Path(p).name
    if stem.endswith(suffix):
        return stem[:-len(suffix)]
    return Path(p).stem


def _load_lengths(annotation_path: Optional[str], level: str = 'gene') -> Tuple[Dict[str, float], Dict[str, float]]:
    if not annotation_path or not os.path.exists(annotation_path):
        return {}, {}
    ext = os.path.splitext(annotation_path)[1].lower()
    df = None
    if ext == '.refflat' and load_refflat_to_dataframe is not None:
        df = load_refflat_to_dataframe(annotation_path)
    else:
        if load_annotation_to_dataframe is not None:
            try:
                df = load_annotation_to_dataframe(annotation_path, file_type='auto')
            except Exception:
                df = None
    if df is None or df.empty:
        return {}, {}
    return build_length_maps(df, level=level)


def write_matrix(samples_data: Dict[str, pd.DataFrame], out_path: str, id_col: str = 'Gene', count_col: str = 'Final_EM', 
                 len_map: Dict[str, float] = None, eff_map: Dict[str, float] = None, quant_type: str = 'tpm') -> None:
    all_ids = set()
    for sample_id, df in samples_data.items():
        if id_col in df.columns and count_col in df.columns:
            all_ids.update(df[id_col].tolist())
    all_ids = sorted(all_ids)

    # 初始化矩阵
    count_mat = pd.DataFrame(index=all_ids)
    
    # 根据定量类型决定是否生成表达量矩阵
    if quant_type in ('tpm', 'rpkm', 'both'):
        expr_mat = pd.DataFrame(index=all_ids)
    else:
        expr_mat = None
    sample_count = 0
    for sample_id, df in samples_data.items():
        sample_count += 1
        print(f"样本{sample_count}: {sample_id} 包含 {len(df)} 个基因")
        if id_col in df.columns and count_col in df.columns:
            s = df.set_index(id_col)[count_col]
            count_mat[sample_id] = count_mat.index.map(lambda g: float(s.get(g, 0.0)))

            # 根据定量类型计算表达量
            if quant_type in ('tpm', 'rpkm', 'both') and expr_mat is not None:
                counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
                
                if quant_type in ('tpm', 'both'):
                    # 计算 TPM
                    tpm = _compute_tpm_series(counts, eff_map or {})
                    expr_mat[f"{sample_id}_tpm"] = expr_mat.index.map(lambda g: float(tpm.get(str(g), 0.0)))
                
                if quant_type in ('rpkm', 'both'):
                    # 计算 RPKM
                    rpkm = _compute_rpkm_series(counts, len_map or {})
                    expr_mat[f"{sample_id}_rpkm"] = expr_mat.index.map(lambda g: float(rpkm.get(str(g), 0.0)))
        else:
            count_mat[sample_id] = 0.0
            if expr_mat is not None:
                if quant_type in ('tpm', 'both'):
                    expr_mat[f"{sample_id}_tpm"] = 0.0
                if quant_type in ('rpkm', 'both'):
                    expr_mat[f"{sample_id}_rpkm"] = 0.0

    # 合并矩阵
    count_mat.columns = [f"{col}_count" for col in count_mat.columns]
    if expr_mat is not None:
        combined = pd.concat([count_mat, expr_mat], axis=1)
    else:
        combined = count_mat

    # 修正：输出前过滤全零/全空行——所有列（计数与TPM/RPKM）均为0或NaN的ID无表达信息，
    #       直接剔除，仅输出至少在一个样本中有值的行；计数全为0的行其TPM/RPKM也必为0，故对整行判断即可
    keep_mask = combined.fillna(0.0).abs().sum(axis=1) > 0
    dropped = int((~keep_mask).sum())
    if dropped:
        print(f"已过滤 {dropped} 个全零/全空行（所有样本中均无计数的ID）")
    combined = combined[keep_mask]

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path)
    print(out_path)


def export_rsem(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM', level: str = 'gene', count_type_suffix: Optional[str] = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        rows = []
        for gid, cnt in counts.items():
            length_val = _safe_float(len_map.get(gid, 0.0), 0.0)
            eff_len_val = _safe_float(eff_map.get(gid, length_val), length_val)
            rows.append((gid, length_val, eff_len_val, _safe_float(cnt, 0.0)))
        if level == 'gene':
            out_df = pd.DataFrame(rows, columns=['gene_id', 'length', 'effective_length', 'expected_count'])
            suffix = f'.{count_type_suffix}' if count_type_suffix else ''
            out_name = f'{sample_id}.rsem.genes{suffix}.results'
        else:
            out_df = pd.DataFrame(rows, columns=['transcript_id', 'length', 'effective_length', 'expected_count'])
            suffix = f'.{count_type_suffix}' if count_type_suffix else ''
            out_name = f'{sample_id}.rsem.isoforms{suffix}.results'
        out_df.to_csv(out_dir / out_name, sep='\t', index=False)


def export_salmon(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM', level: str = 'gene', count_type_suffix: Optional[str] = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        tpm = _compute_tpm_series(counts, eff_map)
        rows = []
        for gid, cnt in counts.items():
            length = _safe_float(len_map.get(gid, 0.0), 0.0)
            eff_len = _safe_float(eff_map.get(gid, length), length)
            tpm_val = float(tpm.get(gid, 0.0))
            rows.append((gid, length, eff_len, float(tpm_val), _safe_float(cnt, 0.0)))
        if level == 'gene':
            out_df = pd.DataFrame(rows, columns=['Name', 'Length', 'EffectiveLength', 'TPM', 'NumReads'])
            suffix = f'.genes.{count_type_suffix}.sf' if count_type_suffix else '.genes.sf'
            out_name = f'{sample_id}.salmon{suffix}'
        else:
            out_df = pd.DataFrame(rows, columns=['Name', 'Length', 'EffectiveLength', 'TPM', 'NumReads'])
            suffix = f'.{count_type_suffix}.quant.sf' if count_type_suffix else '.quant.sf'
            out_name = f'{sample_id}.salmon{suffix}'
        out_df.to_csv(out_dir / out_name, index=False)


def export_kallisto(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM', level: str = 'gene', count_type_suffix: Optional[str] = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        tpm = _compute_tpm_series(counts, eff_map)
        rows = []
        for gid, cnt in counts.items():
            length = _safe_float(len_map.get(gid, 0.0), 0.0)
            eff_len = _safe_float(eff_map.get(gid, length), length)
            tpm_val = float(tpm.get(gid, 0.0))
            rows.append((gid, length, eff_len, _safe_float(cnt, 0.0), float(tpm_val)))
        out_df = pd.DataFrame(rows, columns=['target_id', 'length', 'eff_length', 'est_counts', 'tpm'])
        if level == 'gene':
            suffix = f'.genes.{count_type_suffix}.abundance.tsv' if count_type_suffix else '.genes.abundance.tsv'
        else:
            suffix = f'.isoforms.{count_type_suffix}.abundance.tsv' if count_type_suffix else '.isoforms.abundance.tsv'
        out_df.to_csv(out_dir / f'{sample_id}.kallisto{suffix}', index=False)


def export_featurecounts(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM', level: str = 'gene', count_type_suffix: Optional[str] = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        rows = []
        for gid, cnt in counts.items():
            length = _safe_float(len_map.get(gid, 0.0), 0.0)
            rows.append((gid, length, _safe_float(cnt, 0.0)))
        out_df = pd.DataFrame(rows, columns=['Geneid', 'Length', 'Count'])
        if level == 'gene':
            suffix = f'.genes.{count_type_suffix}.tsv' if count_type_suffix else '.genes.tsv'
        else:
            suffix = f'.isoforms.{count_type_suffix}.tsv' if count_type_suffix else '.isoforms.tsv'
        out_df.to_csv(out_dir / f'{sample_id}.featureCounts{suffix}', sep='\t', index=False)


def run_quant_with_args(args: argparse.Namespace) -> None:
    suffix = '.counts_gene_level_unique.csv' if args.pattern == 'gene_unique' else '.counts_isoform_level_unique.csv'
    processor = PathProcessor()
    files_paths = processor.parse_input_paths(args.inputs, [suffix])
    files = [str(p) for p in files_paths]
    if not files:
        Console(force_terminal=True).print(f"[bold red]❌ 未找到匹配的输入文件: {args.inputs}[/bold red]")
        Console(force_terminal=True).print(f"[bold yellow]要求: {'*.counts_gene_level_unique.csv' if args.pattern == 'gene_unique' else '*.counts_isoform_level_unique.csv'}[/bold yellow]")
        sys.exit(1)
    # 修正：-f 非 none 时需要输出目录；提示语同步 -o/-m 合并后的新语义
    if args.format != 'none' and not args.outdir:
        Console(force_terminal=True).print(f"[bold red]❌ -f 非 none 时需要提供 -o/--outdir 指定输出目录[/bold red]")
        Console(force_terminal=True).print(f"[bold yellow]提示: 若仅需合并计数矩阵，请使用 -f none（矩阵默认输出到 -i 文件夹下，可用 -o 指定输出目录或 .csv 路径），此时 -a 也可省略[/bold yellow]")
        sys.exit(1)
    # 修正：-f none 模式下未指定 -m 时不再报错——矩阵路径默认生成到 -i 输入数据文件夹下（见下方 matrix_path 逻辑）
    # 修正：注释文件仅在需要长度信息时强制——TPM/RPKM 计算（-q 非 none）或格式导出（-f 非 none，文件内含 length/effective_length 列）
    #       纯计数矩阵模式（-q none -f none）可跳过，用户无需准备注释文件
    needs_length = (args.quant != 'none') or (args.format != 'none')
    if needs_length and not args.annotation:
        Console(force_terminal=True).print(f"[bold red]❌ 需要提供 --annotation 才能计算 TPM/RPKM 或导出格式文件[/bold red]")
        Console(force_terminal=True).print(f"[bold yellow]提示: 若仅需合并计数矩阵，请使用 -q none -f none，此时 --annotation 可省略[/bold yellow]")
        sys.exit(1)
    samples: Dict[str, pd.DataFrame] = {}
    for f in files:
        sid = _sample_id_from_path(f, suffix)
        df = pd.read_csv(f)
        samples[sid] = df
    if args.pattern == 'gene_unique':
        id_col_default = 'Gene'
        length_level = 'gene'
    else:
        id_col_default = 'Transcript' if 'Transcript' in next(iter(samples.values())).columns else 'txname'
        length_level = 'isoform'
    # 修正：仅在需要长度信息时解析注释文件——大 GTF/refflat 解析耗时明显，纯矩阵模式直接跳过以加速
    if needs_length:
        len_map, eff_map = _load_lengths(args.annotation, level=length_level)
    else:
        len_map, eff_map = {}, {}
    len_map = {k: _safe_float(v, 0.0) for k, v in len_map.items()}
    eff_map = {k: _safe_float(v, _safe_float(len_map.get(k, 0.0), 0.0)) for k, v in eff_map.items()}
    # 修正：count.py 生成的 CSV 中 unique/multi 计数列名与文件层级有关——
    #       isoform 文件列为 unique_to_isoform / multi_to_isoform，gene 文件列为 unique_to_gene / multi_to_gene；
    #       此处将 -c unique / multi 别名按输入类型解析为实际列名，避免"列不存在"误报
    unique_alias = 'unique_to_gene' if args.pattern == 'gene_unique' else 'unique_to_isoform'
    multi_alias = 'multi_to_gene' if args.pattern == 'gene_unique' else 'multi_to_isoform'

    def _resolve_ct(ct: str) -> str:
        # 修正：列名别名解析——unique/multi 映射为当前文件层级对应的实际列名
        if ct == 'unique':
            return unique_alias
        if ct == 'multi':
            return multi_alias
        return ct

    id_col = id_col_default
    # 修正：-o 与原 -m 合并为统一输出参数——-o 给目录则矩阵输出到该目录（<pattern>_matrix.csv），
    #       给 .csv 文件路径则直接作为矩阵输出（格式文件输出到其所在目录）；未指定时矩阵输出到 -i 数据文件夹下；
    #       -o 无目录部分（纯文件名）时解析到 -i 文件夹下，避免落入当前工作目录找不到文件
    default_dir = Path(files[0]).parent
    outdir = args.outdir
    if outdir:
        if Path(outdir).parent == Path('.'):
            outdir = str(default_dir / outdir)
        if outdir.lower().endswith('.csv'):
            matrix_path = outdir
            outdir = str(Path(matrix_path).parent)
            print(f"矩阵输出到: {matrix_path}")
        else:
            matrix_path = str(Path(outdir) / f"{args.pattern}_matrix.csv")
    else:
        outdir = None
        # 修正：取首个输入文件的父目录作为默认输出目录（多目录输入时以第一个为准）
        matrix_path = str(default_dir / f"{args.pattern}_matrix.csv")
        print(f"未指定 -o，矩阵默认输出到输入数据文件夹: {matrix_path}")
    # 修正：-c 合并原 --columns 功能——取消固定 choices，支持任意 count.py 输出列名与逗号分隔多列；
    #       unique/multi 别名按 -p 解析；矩阵取第一列，格式导出循环全部列；缺失列在运行时报错并列出可用列
    ct_tokens = [c.strip() for c in (args.count_type or 'Final_EM').split(',') if c.strip()]
    if 'all' in ct_tokens:
        # 修正：all 预设中的 unique/multi 替换为当前层级的实际列名（按实际存在列过滤）
        preset = ['raw', unique_alias, multi_alias, 'firstID', 'Final_EM', 'Final_EQ', 'Final_MA']
        present = next(iter(samples.values())).columns
        count_types = [c for c in preset if c in present]
    else:
        count_types = [_resolve_ct(c) for c in ct_tokens]
    ct_for_matrix = count_types[0]
    if ct_for_matrix not in next(iter(samples.values())).columns:
        # 修正：列不存在时列出首个样本的实际可用列，方便用户自查正确列名
        available = list(next(iter(samples.values())).columns)
        Console(force_terminal=True).print(f"[bold yellow]提示:[/bold yellow] 矩阵计数列 {ct_for_matrix} 不存在，跳过生成矩阵")
        Console(force_terminal=True).print(f"[bold yellow]可用列:[/bold yellow] {', '.join(available)}")
    else:
        write_matrix(samples, matrix_path, id_col=id_col, count_col=ct_for_matrix,
                    len_map=len_map, eff_map=eff_map, quant_type=args.quant)
    # 修正：-f none 时不进入格式导出循环，仅生成合并矩阵（纯矩阵模式直接返回）
    if args.format == 'none':
        return
    level_list = ['gene', 'isoform'] if args.level == 'both' else [args.level]
    for lvl in level_list:
        id_col = 'Gene' if (lvl == 'gene') else ('Transcript' if 'Transcript' in next(iter(samples.values())).columns else 'txname')
        for ct in count_types:
            for fmt in ([args.format] if args.format != 'all' else ['rsem', 'salmon', 'kallisto', 'featureCounts']):
                if fmt == 'rsem':
                    export_rsem(samples, len_map, eff_map, outdir, id_col=id_col, count_col=ct, level=lvl, count_type_suffix=ct if args.count_type else None)
                elif fmt == 'salmon':
                    export_salmon(samples, len_map, eff_map, outdir, id_col=id_col, count_col=ct, level=lvl, count_type_suffix=(ct if args.count_type else None))
                elif fmt == 'kallisto':
                    export_kallisto(samples, len_map, eff_map, outdir, id_col=id_col, count_col=ct, level=lvl, count_type_suffix=(ct if args.count_type else None))
                elif fmt == 'featureCounts':
                    export_featurecounts(samples, len_map, outdir, id_col=id_col, count_col=ct, level=lvl, count_type_suffix=(ct if args.count_type else None))


def handle_quant_command(args: argparse.Namespace):
    run_quant_with_args(args)


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """将 quant 模块的公共参数集中定义，避免两处重复维护"""
    parser.add_argument('-i', '--inputs', required=True, help='输入文件或目录（通配符/逗号分隔），gene: *.counts_gene_level_unique.csv；isoform: *.counts_isoform_level_unique.csv')
    # 修正：--annotation 由"必须"改为"条件必须"——仅当需要长度信息（-q 非 none 或 -f 非 none）时才强制，
    #       纯计数矩阵模式（-q none -f none）可省略，方便用户只合并 count 矩阵
    parser.add_argument('-a', '--annotation', required=False, help='注释文件路径：支持 .refflat/.gtf/.gff/.gff3，用于构建长度映射；当 -q 非 none 或 -f 非 none 时必须提供，仅输出计数矩阵（-q none -f none）时可省略')
    parser.add_argument('-p', '--pattern', choices=['gene_unique', 'isoform_unique'], default='gene_unique', help='输入文件类型：gene 或 isoform_unique文件')
    parser.add_argument('-l', '--level', choices=['gene', 'isoform', 'both'], default='gene', help='导出层级：gene/isoform/both.默认gene，当选择文件为gene_unique时，isoform无效')
    # 修正：--format none 语义更新——仅输出合并矩阵（此时 --annotation 可省略）
    parser.add_argument('-f', '--format', choices=['rsem', 'salmon', 'kallisto', 'featureCounts', 'all', 'none'], default='rsem', help='导出格式；none 表示不导出格式文件、仅输出合并矩阵（此时 --annotation 可省略）')
    # 修正：-o 合并原 -m 为统一输出参数——给目录则矩阵与格式文件输出到该目录；给 .csv 路径则直接作为矩阵输出；
    #       未指定时矩阵输出到 -i 数据文件夹下；纯文件名解析到 -i 文件夹下
    parser.add_argument('-o', '--outdir', required=False, help='统一输出参数：给目录 → 格式文件与矩阵输出到该目录（矩阵名 <pattern>_matrix.csv）；给 .csv 文件路径 → 直接作为矩阵输出路径（格式文件输出到其所在目录）；未指定时矩阵输出到 -i 输入数据文件夹下；仅给文件名（无路径）时解析到 -i 文件夹下')
    # 修正：-c 合并原 --columns——取消固定 choices，支持任意 count.py 输出列名与逗号分隔多列（矩阵取第一列）；
    #       unique/multi 为智能别名，依据 -p 自动映射为对应层级的实际列名
    parser.add_argument('-c', '--count-type', default='Final_EM', help='计数列选择：支持任意 count.py 输出的列名（raw/unique_to_isoform/unique_to_gene/multi_to_isoform/multi_to_gene/multi2all/multi_equal/firstID/Final_EM/Final_EQ/Final_MA 等），逗号分隔多列用于格式导出（矩阵取第一列）；unique/multi 为层级别名（按 -p 自动映射为 *_to_isoform 或 *_to_gene）；all 输出全部主要列；默认 Final_EM')
    parser.add_argument('-q', '--quant', choices=['none', 'tpm', 'rpkm', 'both'], default='tpm', help='矩阵中的定量类型：none(仅计数)/tpm/RPKM/both(两者)')

def add_quant_subparser(subparsers):
    parser = subparsers.add_parser(
        'quant',
        help='汇总多个样本并导出格式文件/矩阵',
        # 修正：描述更新——-o 为统一输出参数（目录或 .csv 路径），矩阵默认输出到 -i 文件夹下
        description='输入为 count 生成的 unique CSV: 基因 *.counts_gene_level_unique.csv 或 转录本 *.counts_isoform_level_unique.csv；不支持 multi CSV。默认输出合并矩阵（未指定 -o 时输出到 -i 数据文件夹下，文件名 <pattern>_matrix.csv；-o 可指定输出目录或 .csv 矩阵路径），并可选导出 RSEM/Salmon/Kallisto/featureCounts 格式文件；-f none 时仅输出矩阵（-a 可省略）。',
        formatter_class=CustomHelpFormatter
    )
    _add_common_args(parser)
    parser.set_defaults(func=handle_quant_command)
    return parser



def main():
    parser = argparse.ArgumentParser(
        description='fansetools quant - 汇总多个样本并导出格式文件/矩阵',
        formatter_class=CustomHelpFormatter
    )
    _add_common_args(parser)
    args = parser.parse_args()
    run_quant_with_args(args)


if __name__ == '__main__':
    main()

'''

RNA的高通量测序，搞清楚count matrix如何形成，是否包含multimapped reads，以及MMR的处理如何实现的，这个对差异基因的筛选非常重要，但是这个count步骤往往被忽视。
当count matrix已经形成的时候，最后的分析结果好像已经注定了。反而计算tpm，rpkm这些，大家都是通用的公式，无关紧要。

但是这个最重要的部分是如何形成的，现在市面上的工具纷繁复杂，没有统一的算法和较为详细的说明，甚为遗憾。
这里我们做了这个fanse quant，以及fanse count ，对这一点进行详细的推算。提供了一个统一的框架，可以计算不同种类的count matrix，让人们更了解count的过程，以及可以对比，同一份数据的不同count方案之间的差异，以及差异基因种类有何不同。
'''