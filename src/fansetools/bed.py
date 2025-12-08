#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FANSe3 to BED format converter
Usage: python bed.py input.fanse3 [-o output.bed] [-n 10000]
zhongjiayong，zhaojing
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FANSe3 to BED format converter for fansetools
"""

import os
import argparse
from .utils.rich_help import CustomHelpFormatter
from tqdm import tqdm
from glob import glob

def parse_fanse_line(line1, line2):
    """Parse two-line FANSe3 record into BED entries"""
    fields1 = line1.strip().split('\t')
    fields2 = line2.strip().split('\t')
    
    # Input validation
    if len(fields1) < 2 or len(fields2) < 4:
        raise ValueError(f"Invalid FANSe3 lines:\n{line1}\n{line2}")
    
    tag_name = '#' + fields1[0]
    sequence = fields1[1]
    
    # Process multi-mapping results
    strands = fields2[0].split(',')
    chroms = fields2[1].split(',')
    starts = [s.rstrip('\n') for s in fields2[3].split(',')]
    
    bed_entries = []
    for strand, chrom, start in zip(strands, chroms, starts):
        end = str(int(start) + len(sequence))
        strand = '+' if strand == 'F' else '-'
        bed_entries.append(f"{chrom}\t{start}\t{end}\t{tag_name}\t0\t{strand}\n")
    
    return bed_entries

def process_file(input_path, output_path, max_reads=None):
    """Convert FANSe3 file to BED format"""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    with open(input_path, 'r') as f_in, open(output_path, 'w') as f_out:
        # Estimate total lines for progress bar
        total_lines = sum(1 for _ in f_in) // 2
        f_in.seek(0)
        
        processed = 0
        pbar = tqdm(total=total_lines, desc="Converting")
        
        while True:
            line1 = f_in.readline()
            line2 = f_in.readline()
            
            if not line1 or not line2:
                break
            
            try:
                bed_entries = parse_fanse_line(line1, line2)
                f_out.writelines(bed_entries)
                processed += len(bed_entries)
                pbar.update(1)
                
                if max_reads and processed >= max_reads:
                    break
                    
            except ValueError as e:
                tqdm.write(f"Skipping invalid record: {str(e)}")
                continue
        
        pbar.close()

def fanse2bed(args):
    """Handle the bed subcommand"""
    if os.path.isdir(args.input):
        for f in glob(os.path.join(args.input, args.pattern)):
            output = os.path.splitext(f)[0] + '.bed'
            process_file(f, output, args.max_reads)
    else:
        output = args.output if args.output else \
                 os.path.splitext(args.input)[0] + '.bed'
        process_file(args.input, output, args.max_reads)

def add_bed_subparser(subparsers):
    """Add bed subcommand to the main parser"""
    parser = subparsers.add_parser(
        'bed', 
        help='Convert FANSe3 results to BED format',
        formatter_class=CustomHelpFormatter
    )
    parser.add_argument('-i', '--input', required=True, 
                       help='Input FANSe3 file or directory')
    parser.add_argument('-o', '--output', 
                       help='Output BED file path (for single file input)')
    parser.add_argument('-n', '--max-reads', type=int, 
                       help='Max reads to process')
    parser.add_argument('-p', '--pattern', default='*.fanse3', 
                       help='File pattern for batch processing')
    parser.set_defaults(func=fanse2bed)