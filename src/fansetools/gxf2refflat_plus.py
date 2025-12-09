#!/usr/bin/env python3
"""
Universal GXF (GTF/GFF3) to refFlat Converter with Dual Output
Author: Bioinformatic Tool
Description: Converts GXF files to refFlat format with both genomic and RNA coordinates
Usage: 
- As module: from gxf_refflat_converter import load_annotation_to_dataframe
- As script: python gxf_refflat_converter.py --gxf annotation.gtf output_prefix
"""

import argparse
import sys
import os
import re
import pandas as pd
from collections import defaultdict


#%% common ops
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert GXF (GTF/GFF3) file to extended refFlat format.')
    parser.add_argument('--gxf', required=True, help='Input GXF file (GTF or GFF3)')
    parser.add_argument('output_prefix', help='Output file prefix (will generate .genomic.refflat and .rna.refflat)')
    parser.add_argument('--add-header', action='store_true', help='Add header row to output')
    return parser.parse_args()

def detect_file_type(filename):
    """Auto-detect file type based on extension and content."""
    filename_lower = filename.lower()
    
    # Check extension first
    if filename_lower.endswith('.gtf'):
        return 'gtf'
    elif filename_lower.endswith(('.gff3', '.gff')):
        return 'gff3'
    elif filename_lower.endswith('.refflat'): # Add this line
        return 'refflat'
    
    # Check content
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                if 'gene_id' in line and 'transcript_id' in line:
                    return 'gtf'
                elif 'ID=' in line or 'Parent=' in line:
                    return 'gff3'
                break
    except:
        pass
    
    return 'gff3'  # default to GFF3

def parse_attributes(attr_str, file_type):
    """Parse attributes for both GTF and GFF3."""
    attributes = {}
    
    if file_type == 'gtf':
        # GTF: key "value"; 
        pattern = r'(\w+)\s+"([^"]*)";'
        matches = re.findall(pattern, attr_str)
        for key, value in matches:
            attributes[key] = value
    else:
        # GFF3: key=value;
        for field in attr_str.split(';'):
            field = field.strip()
            if not field  or '=' not in field:
                continue
            if '=' in field:
                key, value = field.split('=', 1)
                value = extract_id(value)
                attributes[key] = value
            else:
                attributes[field] = True
    
    return attributes

def extract_id(id_str, file_type='gff3'):
    """Extract clean ID based on file type."""
    if file_type == 'gtf':
        return id_str.strip('"')
    else:
        prefixes = ['gene:', 'transcript:', 'mrna:', 'cds:', 'exon:','CDS:']
        for prefix in prefixes:
            if id_str.startswith(prefix):
                return id_str[len(prefix):]
        return id_str

def calculate_region_length(intervals):
    """Calculate total length of genomic intervals, merging overlaps."""
    if not intervals or len(intervals) == 0:
        return 0
    
    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    merged = []
    current_start, current_end = sorted_intervals[0]
    
    for start, end in sorted_intervals[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    
    merged.append((current_start, current_end))
    return sum(end - start for start, end in merged)

def calculate_gene_length_metrics(transcripts_df, coverage_intervals=None):
    """计算基因层面的长度指标，标准化列名并补充高级指标
    - genelongesttxLength: 基因内最长转录本长度
    - genelongestcdsLength: 基因内最长CDS长度
    - geneEffectiveLength: 所有转录本外显子并集的非重叠长度
    - geneNonOverlapLength: 与geneEffectiveLength一致，显式提供非重叠外显子长度
    - geneReadCoveredLength: 依据覆盖区间统计的被reads覆盖的外显子长度（无覆盖数据时退化为geneEffectiveLength）
    """
    gene_metrics = {}

    for gene_id, gene_df in transcripts_df.groupby('geneName'):
        # 1. genelonesttxlength: Length of the longest transcript (by txLength)
        longest_tx = gene_df.loc[gene_df['txLength'].idxmax()] if not gene_df.empty else None
        genelonesttxlength = longest_tx['txLength'] if longest_tx is not None else 0
        
        # 2. genelongestcdslength: Length of the longest CDS among all transcripts
        genelongestcdslength = gene_df['cdsLength'].max() if not gene_df['cdsLength'].empty else 0
        
        # 3. geneEffectiveLength: Union length of all exons from all transcripts
        # 不是很对，暂时可以先这样吧，后续在优化。这里应该是所有exons非重叠长度之和
        #或者再多一个长度出来参考
        all_exons = []
        for _, row in gene_df.iterrows():
            starts = row.get('exonStarts_list', [])
            ends = row.get('exonEnds_list', [])
            if starts and ends:
                all_exons.extend(list(zip(starts, ends)))
        non_overlap_len = calculate_region_length(all_exons)
        geneEffectiveLength = int(non_overlap_len if non_overlap_len > 0 else genelongesttxLength)

        # 若提供覆盖区间，则统计与外显子交集的覆盖长度；否则退化为有效长度
        geneReadCoveredLength = geneEffectiveLength
        if coverage_intervals and gene_id in coverage_intervals:
            covered = coverage_intervals[gene_id]
            # 合并覆盖区间后与外显子并集求交长度
            covered_len = 0
            if all_exons:
                # 合并所有外显子为并集区间
                exon_union_len = 0
                merged_exons = []
                for start, end in sorted(all_exons):
                    if not merged_exons or start > merged_exons[-1][1]:
                        merged_exons.append([start, end])
                    else:
                        merged_exons[-1][1] = max(merged_exons[-1][1], end)
                # 计算覆盖区间与外显子并集的交集长度
                merged_cov = []
                for s, e in sorted(covered):
                    if not merged_cov or s > merged_cov[-1][1]:
                        merged_cov.append([s, e])
                    else:
                        merged_cov[-1][1] = max(merged_cov[-1][1], e)
                i = j = 0
                while i < len(merged_exons) and j < len(merged_cov):
                    a_start, a_end = merged_exons[i]
                    b_start, b_end = merged_cov[j]
                    if a_end <= b_start:
                        i += 1
                        continue
                    if b_end <= a_start:
                        j += 1
                        continue
                    inter_start = max(a_start, b_start)
                    inter_end = min(a_end, b_end)
                    if inter_end > inter_start:
                        covered_len += (inter_end - inter_start)
                    if a_end < b_end:
                        i += 1
                    else:
                        j += 1
            geneReadCoveredLength = int(covered_len)

        gene_metrics[gene_id] = {
            'genelongesttxLength': genelongesttxLength,
            'genelongestcdsLength': genelongestcdsLength,
            'geneEffectiveLength': geneEffectiveLength,
            'geneNonOverlapLength': int(non_overlap_len),
            'geneReadCoveredLength': geneReadCoveredLength,
        }

    return gene_metrics

def validate_gene_non_overlap_length(df, sample_genes=None):
    """验证 geneNonOverlapLength 的基本正确性与范围
    - 检查非负
    - 检查不超过所有转录本外显子并集长度
    - 可选：对指定基因重算并集长度进行精确比对
    """
    ok = True
    if 'geneNonOverlapLength' not in df.columns or 'geneName' not in df.columns:
        return False
    # 简单范围检查
    if (df['geneNonOverlapLength'] < 0).any():
        ok = False
    # 精确比对（采样）
    if sample_genes:
        for g in sample_genes:
            sub = df[df['geneName'] == g]
            exons = []
            for _, row in sub.iterrows():
                starts = row.get('exonStarts_list', [])
                ends = row.get('exonEnds_list', [])
                exons.extend(list(zip(starts, ends)))
            union = calculate_region_length(exons)
            expected = int(union)
            observed = int(sub['geneNonOverlapLength'].max()) if not sub.empty else 0
            if observed != expected:
                ok = False
                break
    return ok

def validate_gene_read_covered_length(df):
    """验证 geneReadCoveredLength 的基本正确性
    - 非负
    - 不超过 geneEffectiveLength
    """
    if 'geneReadCoveredLength' not in df.columns:
        return False
    if (df['geneReadCoveredLength'] < 0).any():
        return False
    if 'geneEffectiveLength' in df.columns:
        if (df['geneReadCoveredLength'] > df['geneEffectiveLength']).any():
            return False
    return True

def convert_to_rna_coordinates(genomic_df):
    """
    Convert genomic coordinates to RNA coordinates (0-based relative to transcript start).
    
    In RNA coordinates:
    - txStart is always 0
    - txEnd is the transcript length
    - All coordinates are relative to transcript start
    - Strand information is preserved but coordinates are always from 5' to 3'
    """
    rna_df = genomic_df.copy()
    
    # For RNA coordinates, we reset transcript start to 0
    rna_df['txStart'] = 0
    rna_df['txEnd'] = rna_df['txLength']  # Transcript length

    # Convert CDS coordinates
    rna_df['cdsStart'] = rna_df['utr5Length']  # CDS starts after 5'UTR
    rna_df['cdsEnd'] = rna_df['utr5Length'] + rna_df['cdsLength']
   
    # Remove strand information as all RNA coordinates are 5' to 3'
    rna_df['strand'] = '+'  # All RNA sequences are from 5' to 3'

    # Convert exon coordinates to RNA space
    def convert_exon_coordinates(row):
        # Use the original exon lists, not the string versions
        if 'exonStarts_list' not in row or 'exonEnds_list' not in row:
            return [], []
        
        if not row['exonStarts_list'] or not row['exonEnds_list']:
            return [], []
        
        # Sort exons by genomic position
        exon_data = sorted(zip(row['exonStarts_list'], row['exonEnds_list']))
        
        # Calculate cumulative lengths for RNA coordinates
        rna_starts = []
        rna_ends = []
        current_pos = 0
        
        for exon_start, exon_end in exon_data:
            exon_length = exon_end - exon_start
            rna_starts.append(current_pos)
            rna_ends.append(current_pos + exon_length)
            current_pos += exon_length
        
        return rna_starts, rna_ends
    
    # Apply exon conversion
    exon_conversion = rna_df.apply(convert_exon_coordinates, axis=1, result_type='expand')
    rna_df['exonStarts_list'] = exon_conversion[0]  # Keep as list for formatting
    rna_df['exonEnds_list'] = exon_conversion[1]
    
    # Format exons for output
    def format_exon_columns(row):
        if not row['exonStarts_list'] or not row['exonEnds_list']:
            return 0, '', ''
        
        exon_data = sorted(zip(row['exonStarts_list'], row['exonEnds_list']))
        starts, ends = zip(*exon_data)
        starts_str = ','.join(map(str, starts)) + ','
        ends_str = ','.join(map(str, ends)) + ','
        return len(starts), starts_str, ends_str
    
    exon_info = rna_df.apply(format_exon_columns, axis=1, result_type='expand')
    rna_df['exonCount'] = exon_info[0]
    rna_df['exonStarts'] = exon_info[1]
    rna_df['exonEnds'] = exon_info[2]
    
    # For RNA coordinates, we might want to simplify chromosome naming
    rna_df['chrom'] = rna_df['txname']  # Transcript name 当作染色体号，
    
    return rna_df

def load_refflat_to_dataframe(input_file):
    """
    Load refFlat file and convert to DataFrame.
    Assumes the refFlat file has a specific column order.
    """
    print(f"Processing REFFLAT file: {input_file}")

    # Define the expected columns for the extended refflat format
    # This list should match the output_columns in save_refflat_dataframe
    refflat_columns = [
        'geneName', 'txname', 'chrom', 'strand', 'txStart', 'txEnd',
        'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds',
        'genename', 'g_biotype', 't_biotype', 'protein_id',
        'txLength', 'isoformEffectiveLength', 'cdsLength', 'utr5Length', 'utr3Length',
        'genelongesttxLength', 'genelongestcdsLength', 'geneEffectiveLength',
        'geneNonOverlapLength', 'geneReadCoveredLength', 'description'
    ]

    # Read the refflat file into a DataFrame
    # Skip lines starting with '#' (comments/headers)
    df = pd.read_csv(input_file, sep='\t', comment='#', header=None, names=refflat_columns)

    # Convert relevant columns to appropriate types
    df['txStart'] = df['txStart'].astype(int)
    df['txEnd'] = df['txEnd'].astype(int)
    df['cdsStart'] = df['cdsStart'].astype(int)
    df['cdsEnd'] = df['cdsEnd'].astype(int)
    df['exonCount'] = df['exonCount'].astype(int)
    df['txLength'] = df['txLength'].astype(int)
    # 新增：将 isoformEffectiveLength 转换为整数；缺失时回退为 txLength/0
    if 'isoformEffectiveLength' in df.columns:
        import pandas as _pd
        fallback = df['txLength'] if 'txLength' in df.columns else 0
        df['isoformEffectiveLength'] = _pd.to_numeric(df['isoformEffectiveLength'], errors='coerce').fillna(fallback).astype(int)
    df['cdsLength'] = df['cdsLength'].astype(int)
    df['utr5Length'] = df['utr5Length'].astype(int)
    df['utr3Length'] = df['utr3Length'].astype(int)
    df['genelongesttxLength'] = df['genelongesttxLength'].astype(int)
    df['genelongestcdsLength'] = df['genelongestcdsLength'].astype(int)
    df['geneEffectiveLength'] = df['geneEffectiveLength'].astype(int)
    if 'geneNonOverlapLength' in df.columns:
        df['geneNonOverlapLength'] = df['geneNonOverlapLength'].astype(int)
    if 'geneReadCoveredLength' in df.columns:
        df['geneReadCoveredLength'] = df['geneReadCoveredLength'].astype(int)

    # Reconstruct exonStarts_list and exonEnds_list from string columns
    # These are needed for convert_to_rna_coordinates and track generation
    df['exonStarts_list'] = df['exonStarts'].apply(lambda x: [int(i) for i in x.strip(',').split(',')] if x else [])
    df['exonEnds_list'] = df['exonEnds'].apply(lambda x: [int(i) for i in x.strip(',').split(',')] if x else [])
    
    return df

def load_annotation_to_dataframe(input_file, file_type='auto'):
    """
    Load GXF file and convert to refFlat format DataFrame.
    
    Returns:
        DataFrame with refFlat format columns
    """
    if file_type == 'auto':
        file_type = detect_file_type(input_file)
    
    print(f"Processing {file_type.upper()} file: {input_file}")
    
    transcripts_data = []
    genes_info = {}
    current_transcripts = {}
    
    # 第一阶段：收集基因信息
    with open(input_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line.startswith('#'):
                continue
                
            parts = line.strip().split('\t')
            if len(parts) < 9:
                continue
                
            chrom, source, feature, start, end, score, strand, frame, attributes = parts
            
            # Relevant features
            relevant_features = ['gene', 'mRNA', 'transcript', 'exon', 'CDS', 
                                'five_prime_UTR', 'three_prime_UTR', 'start_codon', 'stop_codon']
            if feature not in relevant_features:
                continue
            
            attr_dict = parse_attributes(attributes, file_type)
            
            # Extract IDs based on file type
            if file_type == 'gtf':
                transcript_id = extract_id(attr_dict.get('transcript_id', ''), file_type)
                gene_id = extract_id(attr_dict.get('gene_id', ''), file_type)
                gene_name = attr_dict.get('gene_name', gene_id)
                biotype = attr_dict.get('gene_biotype', attr_dict.get('biotype', ''))
                description = attr_dict.get('description', '')  # ✅ 添加这行修复
            else:   #当是gff时
                feature_id = extract_id(attr_dict.get('ID', ''), file_type)
                gene_id = feature_id
                gene_name = attr_dict.get('Name', attr_dict.get('gene_name', gene_id))
                biotype = attr_dict.get('biotype', '')    
                parent_id = extract_id(attr_dict.get('Parent', ''), file_type)
                description = attr_dict.get('description', attr_dict.get('Note', ''))
            
            genes_info[gene_id] = {
                'gene_name': gene_name,
                'biotype': biotype,
                'description': description
            }        
            
    # 第二阶段：处理转录本和其他特征
    with open(input_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line.startswith('#'):
                continue
                
            parts = line.strip().split('\t')
            if len(parts) < 9:
                continue
                
            chrom, source, feature, start, end, score, strand, frame, attributes = parts
            
            # Relevant features
            relevant_features = ['gene', 'mRNA', 'transcript', 'exon', 'CDS', 
                                'five_prime_UTR', 'three_prime_UTR', 'start_codon', 'stop_codon']
            if feature not in relevant_features:
                continue
            
            attr_dict = parse_attributes(attributes, file_type)
            
            # Extract IDs based on file type
            if file_type == 'gtf':
                transcript_id = extract_id(attr_dict.get('transcript_id', ''), file_type)
                gene_id = extract_id(attr_dict.get('gene_id', ''), file_type)
                gene_name = attr_dict.get('gene_name', gene_id)
                biotype = attr_dict.get('gene_biotype', attr_dict.get('biotype', ''))
                description = attr_dict.get('description', '')
                protein_id = attr_dict.get('protein_id', '')
            else:
                feature_id = extract_id(attr_dict.get('ID', ''), file_type)
                parent_id = extract_id(attr_dict.get('Parent', ''), file_type)
                
                if feature in ['mRNA', 'transcript']:
                    transcript_id = feature_id
                    gene_id = extract_id(parent_id, ['gene:'])
                else:
                    transcript_id = extract_id(parent_id, ['transcript:', 'mrna:'])
                    gene_id = transcript_id  # Will be updated from transcript
                
                gene_name = attr_dict.get('Name', attr_dict.get('gene_name', gene_id))
                biotype = attr_dict.get('biotype', '')
                description = attr_dict.get('description', attr_dict.get('Note', ''))
                protein_id = attr_dict.get('protein_id', attr_dict.get('Derives_from', ''))
            
            # Handle transcript features
            if feature in ['transcript', 'mRNA']:
                if transcript_id not in current_transcripts:
                    # 获取基因信息，如果不存在则使用默认值
                    gene_data = genes_info.get(gene_id, {})
                    
                    current_transcripts[transcript_id] = {
                        'geneName': gene_id,
                        'txname': transcript_id,
                        'chrom': chrom,
                        'strand': strand,
                        'txStart': int(start) - 1,  # 0-based
                        'txEnd': int(end),
                        'cdsStart': float('inf'),  # Will be updated,先给个默认值
                        'cdsEnd': -1,              # 初始化为极小值，便于后续取最大值
                        'exonStarts_list': [],  # Store as list for calculations
                        'exonEnds_list': [],
                        'cdsRegions': [],
                        'utr5Regions': [],
                        'utr3Regions': [],
                        'genename': gene_data.get('gene_name', gene_name),
                        'g_biotype': gene_data.get('biotype', biotype),
                        't_biotype': attr_dict.get('transcript_biotype', biotype),
                        'protein_id': protein_id,
                        'description': gene_data.get('description', description),
                    }
            
            # Process features for existing transcripts
            if transcript_id in current_transcripts:
                transcript = current_transcripts[transcript_id]
                
                if feature == 'exon':
                    transcript['exonStarts_list'].append(int(start) - 1)
                    transcript['exonEnds_list'].append(int(end))
                
                elif feature == 'CDS':
                    cds_start, cds_end = int(start) - 1, int(end)
                    
                    transcript['cdsStart'] = min(transcript['cdsStart'], cds_start)
                    transcript['cdsEnd'] = max(transcript['cdsEnd'], cds_end)
                    # 在处理完所有特征后，检查CDS是否被设置过
                    if transcript['cdsStart'] == float('inf'):  # 如果没有CDS,或者没有完整的cds，只有start，或者只有end
                        transcript['cdsStart'] = 0
                        transcript['cdsEnd'] = 0
                    transcript['cdsRegions'].append((cds_start, cds_end))
                        
                    # 更新protein_id，如果CDS行中有更准确的信息
                    if not transcript['protein_id'] and 'protein_id' in attr_dict:
                        transcript['protein_id'] = attr_dict['protein_id']
                
                elif feature == 'five_prime_UTR':
                    transcript['utr5Regions'].append((int(start) - 1, int(end)))
                
                elif feature == 'three_prime_UTR':
                    transcript['utr3Regions'].append((int(start) - 1, int(end)))
    
    # Convert to DataFrame
    transcripts_data = list(current_transcripts.values())
    df = pd.DataFrame(transcripts_data)
    
    if df.empty:
        print("Warning: No transcripts found in file")
        return df
    
    # Calculate transcript-level length metrics
    df['txLength'] = df.apply(lambda row: calculate_region_length(
        list(zip(row['exonStarts_list'], row['exonEnds_list']))), axis=1)
    df['cdsLength'] = df['cdsRegions'].apply(calculate_region_length)
    df['utr5Length'] = df['utr5Regions'].apply(calculate_region_length)
    df['utr3Length'] = df['utr3Regions'].apply(calculate_region_length)
    # 新增：isoform 有效长度列（转录本外显子非重叠并集长度），当前等同于 txLength
    # 目的：为 isoform 层的 TPM 归一化提供有效长度
    if 'isoformEffectiveLength' not in df.columns:
        df['isoformEffectiveLength'] = df['txLength']
    
    # Fix CDS coordinates if no CDS found
    mask = df['cdsStart'] > df['cdsEnd']
    df.loc[mask, 'cdsStart'] = df.loc[mask, 'txStart']
    df.loc[mask, 'cdsEnd'] = df.loc[mask, 'txStart']
    
    # Calculate gene-level length metrics
    gene_metrics = calculate_gene_length_metrics(df)
    
    # Add gene-level metrics to transcripts
    df['genelongesttxLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('genelongesttxLength', 0))
    df['genelongestcdsLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('genelongestcdsLength', 0))
    df['geneEffectiveLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('geneEffectiveLength', 0))
    df['geneNonOverlapLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('geneNonOverlapLength', 0))
    df['geneReadCoveredLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('geneReadCoveredLength', 0))
    
    # Prepare exon columns for output
    def prepare_exon_columns(row):
        if not row['exonStarts_list']:
            return 0, '', ''
        
        exon_data = sorted(zip(row['exonStarts_list'], row['exonEnds_list']))
        starts, ends = zip(*exon_data)
        starts_str = ','.join(map(str, starts)) 
        ends_str = ','.join(map(str, ends)) 
        return len(starts), starts_str, ends_str
    
    exon_info = df.apply(prepare_exon_columns, axis=1, result_type='expand')
    df['exonCount'] = exon_info[0]
    df['exonStarts'] = exon_info[1]
    df['exonEnds'] = exon_info[2]

    # IGV refFlat标准列顺序
    igv_columns = [
        'geneName', 'txname', 'chrom', 'strand', 'txStart', 'txEnd',
        'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds'
    ]
    
    # 可选：添加额外信息列
    extra_columns = [
        'genename', 'g_biotype', 't_biotype', 'protein_id',
        'txLength', 'isoformEffectiveLength', 'cdsLength', 'utr5Length', 'utr3Length',
        'genelongesttxLength', 'genelongestcdsLength', 'geneEffectiveLength',
        'geneNonOverlapLength', 'geneReadCoveredLength', 'description',
        'exonStarts_list','exonEnds_list',
    ]
    
    # # Final column order
    # final_columns = [
    #     'geneName', 'txname', 'chrom', 'strand', 'txStart', 'txEnd',
    #     'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts_str', 'exonEnds_str',
    #     'genename', 'g_biotype', 't_biotype', 'protein_id',
    #     'txLength', 'cdsLength', 'utr5Length', 'utr3Length',
    #     'genelonesttxlength', 'genelongestcdslength', 'geneEffectiveLength', 'description'
    # ]
    # 确保所有列都存在
    final_columns = []
    for col in igv_columns + extra_columns:
        if col in df.columns:
            final_columns.append(col)
            
    return df[final_columns] #+ ['exonStarts_list', 'exonEnds_list']]  # Include both string and list versions

def save_refflat_dataframe(df, output_file, add_header=False, is_rna=False):
    """Save DataFrame to refFlat format file."""
    if df.empty:
        print("Warning: Empty DataFrame, nothing to save")
        return
    
    # Select only the string columns for output
    output_columns = [
        'geneName', 'txname', 'chrom', 'strand', 'txStart', 'txEnd',
        'cdsStart', 'cdsEnd', 'exonCount', 'exonStarts', 'exonEnds',
        'genename', 'g_biotype', 't_biotype', 'protein_id',
        'txLength', 'isoformEffectiveLength', 'cdsLength', 'utr5Length', 'utr3Length',
        'genelongesttxLength', 'genelongestcdsLength', 'geneEffectiveLength',
        'geneNonOverlapLength', 'geneReadCoveredLength',
        'description'
    ]
    
    with open(output_file, 'w') as f:
        if add_header:
            header_columns = output_columns 

            f.write("#" + "\t".join(header_columns) + "\n")
        
        df[output_columns].to_csv(f, sep='\t', index=False, header=False)
        # df.to_csv(f, sep='\t', index=False, header=False)
    coord_type = "RNA" if is_rna else "genomic"
    print(f"Saved {len(df)} transcripts to {output_file} ({coord_type} coordinates)")
    
    
def convert_gxf_to_refflat(input_file, output_prefix, file_type='auto', add_header=False):
    """
    Main conversion function - generates both genomic and RNA coordinate files.
    
    Args:
        input_file: Input GXF file path
        output_prefix: Output file prefix
        file_type: 'gtf', 'gff3', or 'auto'
        add_header: Whether to add header row
    
    Returns:
        Tuple of (genomic_df, rna_df) DataFrames
    """
    if file_type == 'auto':
        detected_file_type = detect_file_type(input_file)
    else:
        detected_file_type = file_type

    genomic_df = pd.DataFrame() # Initialize an empty DataFrame

    if detected_file_type == 'refflat':
        # If input is already refflat, load it directly
        genomic_df = load_refflat_to_dataframe(input_file)
        print(f"Input file is already in refflat format. Skipping GXF parsing.")
    elif detected_file_type in ['gtf', 'gff3']:
        # Load genomic coordinates from GXF
        genomic_df = load_annotation_to_dataframe(input_file, detected_file_type)
    else:
        print(f"Error: Unsupported file type '{detected_file_type}' for input file '{input_file}'")
        return None, None
    
    if genomic_df.empty:
        print("Error: No data to process")
        return None, None
    
    # Create RNA coordinates
    rna_df = convert_to_rna_coordinates(genomic_df)
    
    # Save both versions
    genomic_file = f"{output_prefix}.genomic.refflat"
    rna_file = f"{output_prefix}.rna.refflat"
    
    save_refflat_dataframe(genomic_df, genomic_file, add_header, is_rna=False)
    save_refflat_dataframe(rna_df, rna_file, add_header, is_rna=True)
    
    print(f"Generated two refFlat files:")
    print(f"1. Genomic coordinates: {genomic_file}")
    print(f"2. RNA coordinates: {rna_file}")
    print(f"RNA coordinates use 0-based positions relative to transcript start")
    
    return genomic_df, rna_df


###################

def add_exon_numbering_to_rna_refflat(rna_df):
    """
    在RNA坐标的refFlat中添加外显子编号信息
    """
    enhanced_df = rna_df.copy()
    
    def add_exon_numbers(row):
        if not row['exonStarts_list'] or not row['exonEnds_list']:
            return row
        
        # 为每个外显子添加编号
        exon_numbers = []
        for i, (start, end) in enumerate(zip(row['exonStarts_list'], row['exonEnds_list'])):
            exon_numbers.append(f"Exon{i+1}({start}-{end})")
        
        row['exon_labels'] = ';'.join(exon_numbers)
        return row
    
    return enhanced_df.apply(add_exon_numbers, axis=1)

#%%  IGV tracks file generation
def generate_exon_bed_track(genomic_df, output_file):
    """
    生成外显子BED轨道文件，用于IGV中显示外显子边界
    """
    bed_records = []
    
    for _, transcript in genomic_df.iterrows():
        if not transcript['exonStarts_list']:
            continue
            
        # 为每个外显子创建BED记录
        for i, (start, end) in enumerate(zip(transcript['exonStarts_list'], 
                                            transcript['exonEnds_list'])):
            bed_record = {
                'chrom': transcript['chrom'],
                'start': start,
                'end': end,
                'name': f"{transcript['txname']}_exon{i+1}",
                'score': 0,
                'strand': transcript['strand'],
                'thickStart': start,
                'thickEnd': end,
                'itemRgb': f"{i*30},{i*50},{i*70}"  # 不同外显子不同颜色
            }
            bed_records.append(bed_record)
    
    # 保存为BED文件
    bed_df = pd.DataFrame(bed_records)
    bed_df.to_csv(output_file, sep='\t', header=False, index=False)

def generate_exon_gtf_track(rna_df, output_file):
    """
    生成RNA坐标下的外显子GTF轨道文件
    """
    gtf_records = []
    
    for _, transcript in rna_df.iterrows():
        if not transcript['exonStarts_list']:
            continue
            
        # 为每个外显子创建GTF记录
        for i, (start, end) in enumerate(zip(transcript['exonStarts_list'], 
                                            transcript['exonEnds_list'])):
            attributes = (
                f'gene_id "{transcript["geneName"]}"; '
                f'transcript_id "{transcript["txname"]}"; '
                f'exon_number "{i+1}"; '
                f'exon_id "{transcript["txname"]}_exon{i+1}"; '
                f'color "{i*40},{i*60},{i*80}"'
            )
            
            gtf_record = {
                'seqname': transcript['txname'],  # 使用转录本名作为序列名
                'source': 'gxf_converter',
                'feature': 'exon',
                'start': start + 1,  # GTF是1-based
                'end': end,
                'score': '.',
                'strand': '+',  # RNA坐标总是正向
                'frame': '.',
                'attributes': attributes
            }
            gtf_records.append(gtf_record)
    
    # 保存为GTF文件
    gtf_df = pd.DataFrame(gtf_records)
    gtf_df.to_csv(output_file, sep='\t', header=False, index=False, )
                 # quoting=csv.QUOTE_NONE)
    
def generate_ucsc_style_track(genomic_df, output_file):
    """
    生成UCSC风格的基因预测轨道
    """
    # 创建BED12格式，可以显示外显子-内含子结构
    bed12_records = []
    
    for _, transcript in genomic_df.iterrows():
        if not transcript['exonStarts_list']:
            continue
            
        # 排序外显子
        exon_data = sorted(zip(transcript['exonStarts_list'], 
                             transcript['exonEnds_list']))
        starts, ends = zip(*exon_data)
        
        # BED12格式需要计算外显子大小和相对位置
        block_sizes = [end - start for start, end in exon_data]
        block_starts = [start - transcript['txStart'] for start in starts]
        
        bed12_record = {
            'chrom': transcript['chrom'],
            'start': transcript['txStart'],
            'end': transcript['txEnd'],
            'name': transcript['txname'],
            'score': 1000,  # 显示高度
            'strand': transcript['strand'],
            'thickStart': transcript['cdsStart'],
            'thickEnd': transcript['cdsEnd'],
            'itemRgb': '0,0,255',  # 蓝色
            'blockCount': len(exon_data),
            'blockSizes': ','.join(map(str, block_sizes)) + ',',
            'blockStarts': ','.join(map(str, block_starts)) + ','
        }
        bed12_records.append(bed12_record)
    
    # 保存为BED12文件
    bed12_df = pd.DataFrame(bed12_records)
    bed12_df.to_csv(output_file, sep='\t', header=False, index=False)

def convert_gxf_to_refflat_with_tracks(input_file, output_prefix, file_type='auto', add_header=False):
    """
    增强版转换函数：生成refFlat文件和外显子轨道文件
    """
    # 原有的转换逻辑
    genomic_df, rna_df = convert_gxf_to_refflat(input_file, output_prefix, file_type, add_header)
    
    if genomic_df is None:
        return None, None
    
    # 生成外显子轨道文件
    print("生成外显子可视化轨道文件...")
    
    # 1. 基因组坐标的外显子BED轨道
    exon_bed_file = f"{output_prefix}.exons_genomic.bed"
    generate_exon_bed_track(genomic_df, exon_bed_file)
    
    # 2. RNA坐标的外显子GTF轨道
    exon_gtf_file = f"{output_prefix}.exons_rna.gtf"
    generate_exon_gtf_track(rna_df, exon_gtf_file)
    
    # 3. UCSC风格的基因预测轨道
    ucsc_bed_file = f"{output_prefix}.ucsc_style.bed"
    generate_ucsc_style_track(genomic_df, ucsc_bed_file)
    
    print("生成的外显子轨道文件:")
    print(f"1. 基因组外显子BED: {exon_bed_file}")
    print(f"2. RNA外显子GTF: {exon_gtf_file}") 
    print(f"3. UCSC基因预测BED: {ucsc_bed_file}")
    
    return genomic_df, rna_df

def generate_unified_rna_gtf(genomic_df, output_file):
    """
    生成统一的RNA坐标GTF文件，同时满足基因模型和外显子可视化需求
    
    参数:
        genomic_df: 基因组坐标的DataFrame
        output_file: 输出GTF文件路径
    """
    gtf_records = []
    
    for _, transcript in genomic_df.iterrows():
        tx_id = transcript['txname']
        gene_id = transcript['geneName']
        tx_length = transcript['txLength']
        
        # 1. 转录本级别的记录（作为"染色体"定义）
        transcript_record = {
            'seqname': tx_id,  # 使用转录本ID作为序列名
            'source': 'gxf_converter',
            'feature': 'transcript',
            'start': 1,  # RNA坐标从1开始
            'end': tx_length,
            'score': '.',
            'strand': '+',  # RNA坐标总是正向
            'frame': '.',
            'attributes': f'gene_id "{gene_id}"; transcript_id "{tx_id}"; '
                        f'gene_name "{transcript.get("genename", gene_id)}"; '
                        f'biotype "{transcript.get("t_biotype", "")}"; '
                        f'tx_length "{tx_length}"; '
                        f'cds_length "{transcript.get("cdsLength", 0)}"; '
                        f'utr5_length "{transcript.get("utr5Length", 0)}"; '
                        f'utr3_length "{transcript.get("utr3Length", 0)}"'
        }
        gtf_records.append(transcript_record)
        
        # 2. 外显子记录（用于可视化外显子边界）
        if transcript.get('exonStarts_list') and transcript.get('exonEnds_list'):
            for i, (start, end) in enumerate(zip(transcript['exonStarts_list'], 
                                                transcript['exonEnds_list'])):
                # 转换为RNA坐标（0-based到1-based）
                rna_start = start + 1
                rna_end = end
                
                exon_record = {
                    'seqname': tx_id,
                    'source': 'gxf_converter', 
                    'feature': 'exon',
                    'start': rna_start,
                    'end': rna_end,
                    'score': '.',
                    'strand': '+',
                    'frame': '.',
                    'attributes': f'gene_id "{gene_id}"; transcript_id "{tx_id}"; '
                                f'exon_number "{i+1}"; '
                                f'exon_id "{tx_id}_exon{i+1}"; '
                                f'color "{min(255, i*40)},{min(255, i*60)},{min(255, i*80)}"'
                }
                gtf_records.append(exon_record)
        
        # 3. CDS区域记录
        if transcript.get('cdsStart', 0) < transcript.get('cdsEnd', 0):
            cds_start = transcript['cdsStart'] + 1  # 转换为1-based
            cds_end = transcript['cdsEnd']
            
            cds_record = {
                'seqname': tx_id,
                'source': 'gxf_converter',
                'feature': 'CDS', 
                'start': cds_start,
                'end': cds_end,
                'score': '.',
                'strand': '+',
                'frame': '0',  # CDS起始帧
                'attributes': f'gene_id "{gene_id}"; transcript_id "{tx_id}"; '
                            f'protein_id "{transcript.get("protein_id", "")}"'
            }
            gtf_records.append(cds_record)
        
        # 4. UTR区域记录（如果存在）
        if transcript.get('utr5Length', 0) > 0:
            utr5_start = 1
            utr5_end = transcript.get('utr5Length', 0)
            
            utr5_record = {
                'seqname': tx_id,
                'source': 'gxf_converter',
                'feature': 'five_prime_UTR',
                'start': utr5_start,
                'end': utr5_end,
                'score': '.', 
                'strand': '+',
                'frame': '.',
                'attributes': f'gene_id "{gene_id}"; transcript_id "{tx_id}"'
            }
            gtf_records.append(utr5_record)
        
        if transcript.get('utr3Length', 0) > 0:
            utr3_start = tx_length - transcript.get('utr3Length', 0) + 1
            utr3_end = tx_length
            
            utr3_record = {
                'seqname': tx_id,
                'source': 'gxf_converter',
                'feature': 'three_prime_UTR', 
                'start': utr3_start,
                'end': utr3_end,
                'score': '.',
                'strand': '+',
                'frame': '.',
                'attributes': f'gene_id "{gene_id}"; transcript_id "{tx_id}"'
            }
            gtf_records.append(utr3_record)
    
    # 转换为DataFrame并保存
    gtf_df = pd.DataFrame(gtf_records)
    
    # 保存为GTF文件
    gtf_df.to_csv(output_file, sep='\t', header=False, index=False, )
                 # quoting=csv.QUOTE_NONE)
    
    print(f"生成统一RNA GTF文件: {output_file}")
    print(f"包含 {len(gtf_df)} 条记录，涵盖 {len(genomic_df)} 个转录本")
    return gtf_df



def classify_exon(transcript, exon_index, color_by):
    """分类外显子并分配颜色"""
    if color_by == 'type':
        # 基于外显子类型分配颜色
        exon_types = {
            'first': '255,100,100',    # 红色 - 第一个外显子
            'last': '100,100,255',     # 蓝色 - 最后一个外显子  
            'middle': '100,255,100',   # 绿色 - 中间外显子
            'alternative': '255,200,50' # 黄色 - 选择性外显子
        }
        
        total_exons = len(transcript['exonStarts_list'])
        
        if exon_index == 0:
            return 'first', exon_types['first']
        elif exon_index == total_exons - 1:
            return 'last', exon_types['last']
        else:
            return 'middle', exon_types['middle']
    
    elif color_by == 'position':
        # 基于位置渐变颜色
        r = min(255, exon_index * 40)
        g = min(255, exon_index * 60) 
        b = min(255, exon_index * 80)
        return 'exon', f'{r},{g},{b}'
    
    else:
        return 'exon', '100,100,100'
    
def convert_gxf_to_unified_gtf(input_file, output_prefix, file_type='auto', 
                              enhanced=False, add_header=False):
    """
    主转换函数：生成统一的RNA坐标GTF文件
    
    参数:
        input_file: 输入GXF文件
        output_prefix: 输出文件前缀
        file_type: 文件类型检测
        enhanced: 是否使用增强版（支持选择性剪接）
        add_header: 是否添加文件头
    """
    # 1. 解析原始GXF文件
    genomic_df = load_annotation_to_dataframe(input_file, file_type)
    
    if genomic_df.empty:
        print("错误：没有找到可转换的数据")
        return None
    
    # 2. 生成统一GTF文件
    # if enhanced:
    #     output_file = f"{output_prefix}.enhanced.rna.gtf"
    #     gtf_df = generate_enhanced_rna_gtf(genomic_df, output_file, 
    #                                      include_splicing_events=True)
    # else:
    output_file = f"{output_prefix}.rna.gtf" 
    gtf_df = generate_unified_rna_gtf(genomic_df, output_file)
    
    # 3. 可选：生成基因组坐标的refFlat作为备份
    genomic_file = f"{output_prefix}.genomic.refflat"
    save_refflat_dataframe(genomic_df, genomic_file, add_header, is_rna=False)
    
    print("转换完成！")
    print(f"主要输出: {output_file} (统一RNA GTF文件)")
    print(f"备用输出: {genomic_file} (基因组坐标refFlat)")
    print("\nIGV使用说明:")
    print("1. 加载RNA GTF文件作为注释轨道")
    print("2. 选择任意转录本ID作为参考序列")
    print("3. 外显子将自动显示不同颜色和编号")
    
    return gtf_df


#%% main function
def main():
    """Command line interface."""
    args = parse_arguments()
    
    if not os.path.exists(args.gxf):
        print(f"Error: Input file {args.gxf} does not exist.")
        sys.exit(1)
    
    # Auto-detect file type
    file_type = detect_file_type(args.gxf)
    print(f"Detected file type: {file_type.upper()}")
    
    # Generate output files    #两个refflat文件，rna没有分割bed
    # genomic_df, rna_df = convert_gxf_to_refflat(
    #     args.gxf, args.output_prefix, file_type, args.add_header
    # )
    #生成两个refflat文件，再加上RNA.bed, 先这样子先
    genomic_df, rna_df = convert_gxf_to_refflat_with_tracks(
        args.gxf, args.output_prefix, file_type, args.add_header
    )    
    # genomic_df, rna_df =  convert_gxf_to_refflat_with_tracks(input_gff, output_prefix, file_type='auto', add_header=True)
    
    
    if genomic_df is not None:
        print(f"Conversion complete. Processed {len(genomic_df)} transcripts.")
        print(f"Gene-level length metrics available:")
        print(f"  - genelonesttxlength: Longest transcript length per gene")
        print(f"  - genelongestcdslength: Longest CDS length per gene")
        print(f"  - geneEffectiveLength: Gene effective length for normalization")

# For module import
def load_gxf_to_dataframe(input_file, file_type='auto'):
    """Alias for load_annotation_to_dataframe for backward compatibility."""
    return load_annotation_to_dataframe(input_file, file_type)

if __name__ == '__main__':
    main()
    
    # input_file = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311.gff'    
    # input_gff = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311.gff'
    # output_prefix = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311_tracks'
    # add_header = 1
    # genomic_df, rna_df = convert_gxf_to_refflat(input_gff, output_prefix, file_type='auto', add_header=True)

    # genomic_df, rna_df =  convert_gxf_to_refflat_with_tracks(input_gff, output_prefix, file_type='auto', add_header=True)


    # # 基本使用
    # rna_gtf = convert_gxf_to_unified_gtf(
    #     input_file= input_gff , 
    #     output_prefix = output_prefix ,
    #     file_type="auto"
    # )
    # （移除错误示例代码）
