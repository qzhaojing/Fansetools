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
            if not field:
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

def calculate_gene_length_metrics(transcripts_df):
    """Calculate three gene-level length metrics."""
    gene_metrics = {}
    
    for gene_id, gene_df in transcripts_df.groupby('geneName'):
        # 1. genelonesttxlength: Length of the longest transcript (by txLength)
        longest_tx = gene_df.loc[gene_df['txLength'].idxmax()] if not gene_df.empty else None
        genelonesttxlength = longest_tx['txLength'] if longest_tx is not None else 0
        
        # 2. genelongestcdslength: Length of the longest CDS among all transcripts
        genelongestcdslength = gene_df['cdsLength'].max() if not gene_df['cdsLength'].empty else 0
        
        # 3. geneEffectiveLength: Effective length for normalization (longest CDS)
        geneEffectiveLength = genelongestcdslength
        
        gene_metrics[gene_id] = {
            'genelonesttxlength': genelonesttxlength,
            'genelongestcdslength': genelongestcdslength,
            'geneEffectiveLength': geneEffectiveLength
        }
    
    return gene_metrics

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
                        'cdsStart': int(end),  # Will be updated
                        'cdsEnd': int(start) - 1,
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
    
    # Fix CDS coordinates if no CDS found
    mask = df['cdsStart'] > df['cdsEnd']
    df.loc[mask, 'cdsStart'] = df.loc[mask, 'txStart']
    df.loc[mask, 'cdsEnd'] = df.loc[mask, 'txStart']
    
    # Calculate gene-level length metrics
    gene_metrics = calculate_gene_length_metrics(df)
    
    # Add gene-level metrics to transcripts
    df['genelonesttxlength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('genelonesttxlength', 0))
    df['genelongestcdslength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('genelongestcdslength', 0))
    df['geneEffectiveLength'] = df['geneName'].map(
        lambda x: gene_metrics.get(x, {}).get('geneEffectiveLength', 0))
    
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
        'txLength', 'cdsLength', 'utr5Length', 'utr3Length',
        'genelonesttxlength', 'genelongestcdslength', 'geneEffectiveLength', 'description',
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
        'txLength', 'cdsLength', 'utr5Length', 'utr3Length',
        'genelonesttxlength', 'genelongestcdslength', 'geneEffectiveLength',
        'description'
    ]
    
    with open(output_file, 'w') as f:
        if add_header:
            header_columns = output_columns 
            # [
            #     "geneName", "txname", "chrom", "strand", "txStart", "txEnd", 
            #     "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds",
            #     "genename", "g_biotype", "t_biotype", "protein_id",
            #     "txLength", "cdsLength", "utr5Length", "utr3Length",
            #     "genelonesttxlength", "genelongestcdslength", "geneEffectiveLength",
            #      'description'
            # ]
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
    # Load genomic coordinates
    genomic_df = load_annotation_to_dataframe(input_file, file_type)
    
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

def main():
    """Command line interface."""
    args = parse_arguments()
    
    if not os.path.exists(args.gxf):
        print(f"Error: Input file {args.gxf} does not exist.")
        sys.exit(1)
    
    # Auto-detect file type
    file_type = detect_file_type(args.gxf)
    print(f"Detected file type: {file_type.upper()}")
    
    # Generate output files
    genomic_df, rna_df = convert_gxf_to_refflat(
        args.gxf, args.output_prefix, file_type, args.add_header
    )
    
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
    
    
    input_gff = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311.gff'
    output_prefix = r'\\fs2\D\DATA\Zhaojing\202209数据汇缴\0.Ref_seqs\20251024Oryza_sativa.IRGSP_9311'
    add_header = 1
    genomic_df, rna_df = convert_gxf_to_refflat(input_gff, output_prefix, file_type='auto', add_header=True)
