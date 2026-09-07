# FanseTools

**FanseTools** is a comprehensive toolkit designed for processing and analyzing outputs from the FANSe algorithm—a high-accuracy solution for next-generation sequencing (NGS) data analysis.

---

## Introduction

FANSe outputs results in its own format for easier analysis and processing. This toolkit enables seamless conversion, parsing, and further manipulation of FANSe-formatted files. It also supports conversion to commonly used formats (SAM, BAM, BED, FASTQ) for compatibility with other bioinformatics tools.

For detailed information on the FANSe3 output format, please see:  
[Fanse3_output_style.MD](https://github.com/qzhaojing/Fansetools/blob/main/Fanse3_output_style.MD)

---

## Key Features

- **Format Conversion:** Convert FANSe3 outputs to SAM, BAM, BED, FASTQ, and back.
- **Count & RPKM Calculations:** Directly obtain read counts and calculate RPKM at gene or transcript level.
- **Pipeline Integration:** Streamlined processing from FASTQ to downstream differential analysis.
- **Parser Utilities:** Format and prepare FANSe3 files for customized downstream analyses.
- **Stream Processing:** Supports high-throughput, streaming data conversion for efficiency.
- **Planned Features:** Fusion gene detection, paired-end support, and more.

---

## Installation

1. **Clone the repository:**
   ```
   git clone https://github.com/qzhaojing/Fansetools.git
   ```

2. **Enter the directory:**
   ```
   cd Fansetools
   ```

3. **Install the package:**
   ```
   python setup.py install
   ```

On windows, recommand install as follows:
```bash
pip install git+https://github.com/qzhaojing/Fansetools.git
```
or
```
git install https://github.com/qzhaojing/Fansetools/archive/refs/tags/v1.0.0.tar.gz
```



After installation, you can invoke `fansetools` directly from the command line.
Now type fanse to start your analysis.
```
fanse
```

---

## Usage Overview

### General Command Structure

```bash
conda install fansetools
```

If you are developing from source inside a conda environment:

```bash
conda activate base
pip install -e .
```

## Workflow examples

### Example 1 RNA-seq / RNC-seq end-to-end workflow

Start from `fq.gz`, trim, align, count, and quantify:

```bash
fanse trim -i sample.fq.gz -o sample.trim.fq.gz
fanse run -i sample.trim.fq.gz -r reference.fasta -o sample.fanse3
fanse count -i sample.fanse3 -o counts_dir
fanse quant -i counts_dir -p isoform_unique -c unique -q none -f none
```

- **Unmapped Reads to FASTQ:**
  ```bash
  unmapped2fastq -i input.fanse3 -o output_unmapped.fastq
  ```

Count reads in selected regions using a BED file:

- **Count reads per gene or transcript:**
  ```bash
  fanse2count -i input.fanse3 -o counts.txt
  # Options:
  #   --gene_level         # for gene-level counts
  #   --transcript_level   # for transcript-level counts
  ```

### Example 3 Multi-sample matrix aggregation

Aggregate multiple sample count files into a matrix for downstream analysis:

```bash
fanse quant -i counts_dir -p isoform_unique -c multi_to_isoform -q none -f none
```

## Release notes

Current target release: v1.2.0
