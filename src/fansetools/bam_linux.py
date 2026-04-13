# -*- coding: utf-8 -*-
import sys
import os
import pysam
import time
import argparse
from multiprocessing import Pool, cpu_count
from typing import List, Dict, Any, Tuple
from tqdm import tqdm

# Import from package modules
try:
    from .parser import parse_records_from_lines, fanse_line_reader
    from .sam import calculate_mapq_advanced, generate_cigar, calculate_flag, reverse_complement, calculate_nm
except ImportError:
    # Fallback for when running as script outside package
    # This assumes fansetools is in python path or installed
    from fansetools.parser import parse_records_from_lines, fanse_line_reader
    from fansetools.sam import calculate_mapq_advanced, generate_cigar, calculate_flag, reverse_complement, calculate_nm

def alignment_to_tuples(alignment: str, is_reverse: bool) -> List[Tuple[int, int]]:
    """
    Convert FANSe alignment string to pysam CIGAR tuples.
    """
    if is_reverse:
        alignment = alignment[::-1]
    
    if not alignment:
        return []
    
    tuples = []
    # pysam cigar ops: M:0, I:1, D:2, N:3, S:4, H:5, P:6, =:7, X:8
    # FANSe: .:Match, x:Mismatch, -:Deletion, others:Insertion/Softclip
    
    current_op = None
    count = 0
    
    for char in alignment:
        if char == '.': op = 0    # M (Match)
        elif char == 'x': op = 8  # X (Mismatch)
        elif char == '-': op = 2  # D (Deletion)
        elif char.isalpha(): op = 1 # I (Insertion)
        else: op = 4              # S (Softclip)
        
        # Optimize: if using M for both match and mismatch (standard SAM often uses M)
        # But here we map x to X (8). If compatibility is needed, map x to 0 (M).
        # sam.py generates 'X' for 'x'. So we use 8.
        
        if current_op is None:
            current_op = op
            count = 1
        elif op == current_op:
            count += 1
        else:
            tuples.append((current_op, count))
            current_op = op
            count = 1
            
    if current_op is not None:
        tuples.append((current_op, count))
        
    return tuples

def fanse_to_pysam_data(record) -> List[Dict[str, Any]]:
    """
    Convert FANSeRecord to a list of dictionaries for pysam.AlignedSegment creation.
    Implements full logic from sam.py including MAPQ and multi-mapping.
    """
    if not record.ref_names:
        return []

    results = []
    
    # Primary alignment selection (same as sam.py)
    primary_idx = 0
    # primary_strand = record.strands[primary_idx]
    
    # Pre-calculate MAPQs using advanced logic from sam.py
    mapq_values = []
    for i in range(len(record.ref_names)):
        is_primary = (i == primary_idx)
        mapq = calculate_mapq_advanced(record, i, is_primary)
        mapq_values.append(mapq)
        
    # Process all alignments
    for i in range(len(record.ref_names)):
        ref_name = record.ref_names[i]
        pos = record.positions[i] # 0-based
        strand = record.strands[i]
        is_rev = (strand == 'R')
        
        # Flags
        is_primary = (i == primary_idx)
        is_secondary = not is_primary
        flag = calculate_flag(strand, is_secondary=is_secondary)
        
        # Sequence handling
        seq = record.seq
        if is_rev:
            seq = reverse_complement(seq)
            
        # CIGAR
        cigartuples = alignment_to_tuples(record.alignment[i], is_rev)
        
        # Tags
        tags = []
        tags.append(('NM', calculate_nm(record.alignment[i])))
        tags.append(('XM', record.mismatches[i]))
        tags.append(('XN', record.multi_count))
        tags.append(('XS', mapq_values[i]))
        
        # Note: SA tag omitted for performance, can be added if needed
        
        results.append({
            'qname': record.header,
            'seq': seq,
            'flag': flag,
            'ref_name': ref_name,
            'pos': pos,
            'mapq': mapq_values[i],
            'cigar': cigartuples,
            'tags': tags
        })
        
    return results

def process_chunk(lines):
    """
    Worker function: Parse raw lines and convert to pysam-ready data dicts.
    """
    results = []
    # Use parser from sam.py ecosystem
    for record in parse_records_from_lines(lines):
        results.extend(fanse_to_pysam_data(record))
    return results

def fanse2bam_pysam(fanse_file, fasta_path, output_bam, threads=1):
    # 1. 读取参考基因组构建Header
    print("Reading reference...")
    with pysam.FastaFile(fasta_path) as fasta:
        header = { 'HD': {'VN': '1.0', 'SO': 'coordinate'}, 'SQ': [] }
        for ref in fasta.references:
            header['SQ'].append({'SN': ref, 'LN': fasta.get_reference_length(ref)})
    
    print(f"Converting with {threads} threads (Streaming mode)...")
    start_time = time.time()
    count = 0
    
    # 2. 打开输出BAM
    # 启用pysam的内部线程进行BGZF压缩
    pysam_threads = threads if threads > 1 else 0
    
    with pysam.AlignmentFile(output_bam, "wb", header=header, threads=pysam_threads) as outf:
        
        # 准备数据源
        batch_size = 100000
        reader = fanse_line_reader(fanse_file, chunk_size=batch_size)
        file_size = os.path.getsize(fanse_file)
        # Estimate total reads for progress bar (approx 450 bytes per read)
        total_reads_est = file_size / 450
        
        if threads > 1:
            with Pool(processes=threads) as pool:
                with tqdm(total=total_reads_est, unit='reads', unit_scale=True) as pbar:
                    # imap streaming
                    for batch_results in pool.imap(process_chunk, reader, chunksize=1):
                        for data in batch_results:
                            a = pysam.AlignedSegment(outf.header)
                            a.query_name = data['qname']
                            a.query_sequence = data['seq']
                            a.flag = data['flag']
                            a.reference_name = data['ref_name']
                            a.reference_start = data['pos']
                            a.mapping_quality = data['mapq']
                            a.cigartuples = data['cigar']
                            a.query_qualities = [30] * len(data['seq']) # Dummy qual
                            a.set_tags(data['tags'])
                            outf.write(a)
                            count += 1
                        
                        # Update progress based on processed chunk (approx)
                        pbar.update(batch_size // 2)
        else:
            # Single thread mode
            with tqdm(total=total_reads_est, unit='reads', unit_scale=True) as pbar:
                for lines in reader:
                    batch_results = process_chunk(lines)
                    for data in batch_results:
                        a = pysam.AlignedSegment(outf.header)
                        a.query_name = data['qname']
                        a.query_sequence = data['seq']
                        a.flag = data['flag']
                        a.reference_name = data['ref_name']
                        a.reference_start = data['pos']
                        a.mapping_quality = data['mapq']
                        a.cigartuples = data['cigar']
                        a.query_qualities = [30] * len(data['seq'])
                        a.set_tags(data['tags'])
                        outf.write(a)
                        count += 1
                    pbar.update(batch_size // 2)

    end_time = time.time()
    duration = end_time - start_time
    print(f"Finished. Total: {count}, Time: {duration:.2f}s, Speed: {count/duration:.2f} reads/s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FANSe to BAM converter (Linux Optimized v0.2)")
    parser.add_argument('fanse_file', help='Input FANSe3 file')
    parser.add_argument('fasta_path', help='Reference FASTA file')
    parser.add_argument('output_bam', help='Output BAM file')
    parser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads (default: 1)')
    
    args = parser.parse_args()
    
    fanse2bam_pysam(args.fanse_file, args.fasta_path, args.output_bam, args.threads)
