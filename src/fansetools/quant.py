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
    if not args.annotation:
        Console(force_terminal=True).print(f"[bold red]❌ 需要提供 --annotation 才能计算长度与TPM[/bold red]")
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
    len_map, eff_map = _load_lengths(args.annotation, level=length_level)
    len_map = {k: _safe_float(v, 0.0) for k, v in len_map.items()}
    eff_map = {k: _safe_float(v, _safe_float(len_map.get(k, 0.0), 0.0)) for k, v in eff_map.items()}
    id_col = id_col_default
    if args.matrix:
        ct_for_matrix = args.count_type if (args.count_type and args.count_type != 'all') else (args.columns.split(',')[0] if args.columns else 'Final_EM')
        if ct_for_matrix not in next(iter(samples.values())).columns:
            Console(force_terminal=True).print(f"[bold yellow]提示:[/bold yellow] 矩阵计数列 {ct_for_matrix} 不存在，跳过生成矩阵")
        else:
            write_matrix(samples, args.matrix, id_col=id_col, count_col=ct_for_matrix, 
                        len_map=len_map, eff_map=eff_map, quant_type=args.quant)
    outdir = args.outdir
    if args.count_type and args.count_type != 'all':
        count_types = [args.count_type]
    elif args.columns:
        count_types = [c.strip() for c in args.columns.split(',') if c.strip()]
    else:
        count_types = ['Final_EM']
    if args.count_type == 'all':
        preset = ['raw', 'unique', 'firstID', 'Final_EM', 'Final_EQ', 'Final_MA']
        present = next(iter(samples.values())).columns
        count_types = [c for c in preset if c in present]
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
    parser.add_argument('-a', '--annotation', required=True, help='注释文件路径（必须）：支持 .refflat/.gtf/.gff/.gff3，将统一构建长度映射')
    parser.add_argument('-p', '--pattern', choices=['gene_unique', 'isoform_unique'], default='gene_unique', help='输入文件类型：gene 或 isoform_unique文件')
    parser.add_argument('-l', '--level', choices=['gene', 'isoform', 'both'], default='gene', help='导出层级：gene/isoform/both.默认gene，当选择文件为gene_unique时，isoform无效')
    parser.add_argument('-f', '--format', choices=['rsem', 'salmon', 'kallisto', 'featureCounts', 'all'], default='rsem', help='导出格式')
    parser.add_argument('-o', '--outdir', required=True, help='输出目录')
    parser.add_argument('-m', '--matrix', required=False, help='可选：输出合并矩阵 CSV 路径（使用首个计数列）')
    parser.add_argument('-c', '--count-type', choices=['raw', 'unique', 'firstID', 'Final_EM', 'Final_EQ', 'Final_MA', 'all'], default='Final_EM', help='计数列选择；all 输出多份')
    parser.add_argument('-q', '--quant', choices=['none', 'tpm', 'rpkm', 'both'], default='tpm', help='矩阵中的定量类型：none(仅计数)/tpm/RPKM/both(两者)')
    parser.add_argument('--columns', default='Final_EM', help='兼容参数：逗号分隔的计数列；与 --count-type 同时出现时以 --count-type 为准')

def add_quant_subparser(subparsers):
    parser = subparsers.add_parser(
        'quant',
        help='汇总多个样本并导出格式文件/矩阵',
        description='输入为 count 生成的 unique CSV: 基因 *.counts_gene_level_unique.csv 或 转录本 *.counts_isoform_level_unique.csv；不支持 multi CSV。支持导出 RSEM/Salmon/Kallisto/featureCounts 及合并矩阵。',
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