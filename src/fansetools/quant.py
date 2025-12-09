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

import pandas as pd
import math

try:
    from .gxf2refflat_plus import load_refflat_to_dataframe
except Exception:
    load_refflat_to_dataframe = None


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
        length_map = dict(zip(annotation_df[id_col], annotation_df[len_col]))
        if eff_col:
            eff_length_map_raw = dict(zip(annotation_df[id_col], annotation_df[eff_col]))
            # 将无效(<=0或NA)的有效长度替换为总长度
            eff_length_map = {k: (float(v) if (pd.notna(v) and float(v) > 0) else float(length_map.get(k, 0.0))) for k, v in eff_length_map_raw.items()}
        else:
            eff_length_map = length_map
        return length_map, eff_length_map

    # gene level
    if 'geneName' not in annotation_df.columns:
        return {}, {}
    # gene 层长度选择
    mode_gene = mode or 'genelongesttxLength'
    if mode_gene == 'geneEffectiveLength' and 'geneEffectiveLength' in annotation_df.columns:
        length_map = dict(annotation_df.groupby('geneName')['geneEffectiveLength'].max())
    elif mode_gene == 'genelongestcdsLength' and 'genelongestcdsLength' in annotation_df.columns:
        length_map = dict(annotation_df.groupby('geneName')['genelongestcdsLength'].max())
    elif mode_gene == 'txLength' and 'txLength' in annotation_df.columns:
        length_map = dict(annotation_df.groupby('geneName')['txLength'].max())
    elif 'genelongesttxLength' in annotation_df.columns:
        length_map = dict(annotation_df.groupby('geneName')['genelongesttxLength'].max())
    elif 'txLength' in annotation_df.columns:
        length_map = dict(annotation_df.groupby('geneName')['txLength'].max())
    else:
        length_map = {}

    # 修正：若存在 geneEffectiveLength，则使用它；但对<=0/NA值回退到总长度，避免TPM为0
    if 'geneEffectiveLength' in annotation_df.columns:
        eff_raw = dict(annotation_df.groupby('geneName')['geneEffectiveLength'].max())
        eff_length_map = {k: (float(v) if (pd.notna(v) and float(v) > 0) else float(length_map.get(k, 0.0))) for k, v in eff_raw.items()}
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

def _sample_id_from_path(p: str, suffix: str) -> str:
    stem = Path(p).name
    if stem.endswith(suffix):
        return stem[:-len(suffix)]
    return Path(p).stem


def _load_gene_lengths(annotation_path: Optional[str]) -> Tuple[Dict[str, float], Dict[str, float]]:
    if not annotation_path or not os.path.exists(annotation_path):
        return {}, {}
    if load_refflat_to_dataframe is None:
        return {}, {}
    df = load_refflat_to_dataframe(annotation_path)
    return build_length_maps(df, level='gene')


def write_matrix(samples_data: Dict[str, pd.DataFrame], out_path: str, id_col: str = 'Gene', count_col: str = 'Final_EM') -> None:
    all_ids = set()
    for df in samples_data.values():
        if id_col in df.columns and count_col in df.columns:
            all_ids.update(df[id_col].tolist())
    all_ids = sorted(all_ids)
    mat = pd.DataFrame(index=all_ids)
    for sample_id, df in samples_data.items():
        s = df.set_index(id_col)[count_col] if id_col in df.columns and count_col in df.columns else pd.Series(dtype=float)
        mat[sample_id] = mat.index.map(lambda g: float(s.get(g, 0.0)))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    mat.to_csv(out_path)


def export_rsem(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM') -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        rows = []
        for gid, cnt in counts.items():
            length = len_map.get(gid, 0.0)
            eff_len = eff_map.get(gid, length)
            rows.append((gid, length, eff_len, float(cnt)))
        out_df = pd.DataFrame(rows, columns=['gene_id', 'length', 'effective_length', 'expected_count'])
        out_df.to_csv(out_dir / f'{sample_id}.rsem.genes.results', sep='\t', index=False)


def export_salmon(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM') -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        tpm = _compute_tpm_series(counts, eff_map)
        rows = []
        for gid, cnt in counts.items():
            length = len_map.get(gid, 0.0)
            eff_len = eff_map.get(gid, length)
            tpm_val = float(tpm.get(gid, 0.0))
            rows.append((gid, length, eff_len, float(cnt), float(tpm_val)))
        out_df = pd.DataFrame(rows, columns=['Name', 'Length', 'EffectiveLength', 'NumReads', 'TPM'])
        out_df.to_csv(out_dir / f'{sample_id}.quant.sf', sep='\t', index=False)


def export_kallisto(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], eff_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM') -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        tpm = _compute_tpm_series(counts, eff_map)
        rows = []
        for gid, cnt in counts.items():
            length = len_map.get(gid, 0.0)
            eff_len = eff_map.get(gid, length)
            tpm_val = float(tpm.get(gid, 0.0))
            rows.append((gid, length, eff_len, float(cnt), float(tpm_val)))
        out_df = pd.DataFrame(rows, columns=['target_id', 'length', 'eff_length', 'est_counts', 'tpm'])
        out_df.to_csv(out_dir / f'{sample_id}.abundance.tsv', sep='\t', index=False)


def export_featurecounts(samples_data: Dict[str, pd.DataFrame], len_map: Dict[str, float], out_dir: str, id_col: str = 'Gene', count_col: str = 'Final_EM') -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for sample_id, df in samples_data.items():
        counts = df.set_index(id_col)[count_col] if (id_col in df.columns and count_col in df.columns) else pd.Series(dtype=float)
        rows = []
        for gid, cnt in counts.items():
            length = len_map.get(gid, 0.0)
            rows.append((gid, length, float(cnt)))
        out_df = pd.DataFrame(rows, columns=['Geneid', 'Length', 'Counts'])
        out_df.to_csv(out_dir / f'{sample_id}.featureCounts.txt', sep='\t', index=False)


def main():
    parser = argparse.ArgumentParser(
        description='fansetools quant - 汇总多个样本并导出格式文件/矩阵',
        formatter_class=CustomHelpFormatter
    )
    parser.add_argument('--inputs', required=True, help='输入文件或目录，支持通配符和逗号分隔')
    parser.add_argument('--annotation', required=False, help='refflat 注释文件路径')
    parser.add_argument('--pattern', choices=['gene_unique','isoform_unique'], default='gene_unique', help='输入文件类型：gene 或 isoform 唯一文件')
    parser.add_argument('--columns', default='Final_EM', help='用于矩阵与表达量计算的计数列，逗号分隔')
    parser.add_argument('--format', choices=['rsem', 'salmon', 'kallisto', 'featureCounts', 'all'], default='rsem')
    parser.add_argument('--outdir', required=True, help='输出目录')
    parser.add_argument('--matrix', required=False, help='可选：输出合并矩阵 CSV 路径')
    args = parser.parse_args()

    suffix = '.counts_gene_level_unique.csv' if args.pattern == 'gene_unique' else '.counts_isoform_level_unique.csv'
    
    processor = PathProcessor()
    files_paths = processor.parse_input_paths(args.inputs, [suffix])
    files = [str(p) for p in files_paths]
    
    if not files:
        Console(force_terminal=True).print(f"[bold red]❌ 未找到匹配的文件: {args.inputs} (需包含后缀: {suffix})[/bold red]")
        sys.exit(1)

    samples: Dict[str, pd.DataFrame] = {}
    for f in files:
        sid = _sample_id_from_path(f, suffix)
        df = pd.read_csv(f)
        samples[sid] = df

    id_col = 'Gene' if args.pattern == 'gene_unique' else ('Transcript' if 'Transcript' in next(iter(samples.values())).columns else 'txname')
    count_cols = [c.strip() for c in args.columns.split(',') if c.strip()]

    len_map, eff_map = _load_gene_lengths(args.annotation) if args.pattern == 'gene_unique' else build_length_maps(next(iter(samples.values())), level='isoform')

    # 生成矩阵（仅使用第一个计数列）
    if args.matrix:
        write_matrix(samples, args.matrix, id_col=id_col, count_col=count_cols[0])

    outdir = args.outdir
    if args.format in ['rsem', 'all']:
        export_rsem(samples, len_map, eff_map, outdir, id_col=id_col, count_col=count_cols[0])
    if args.format in ['salmon', 'all']:
        export_salmon(samples, len_map, eff_map, outdir, id_col=id_col, count_col=count_cols[0])
    if args.format in ['kallisto', 'all']:
        export_kallisto(samples, len_map, eff_map, outdir, id_col=id_col, count_col=count_cols[0])
    if args.format in ['featureCounts', 'all']:
        export_featurecounts(samples, len_map, outdir, id_col=id_col, count_col=count_cols[0])


if __name__ == '__main__':
    main()

