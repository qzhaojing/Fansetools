# FanseTools

FanseTools is a FANSe3 processing toolkit for alignment, conversion, counting, quantification, and workflow automation.

## Main capabilities

- `fanse trim`: FASTQ quality control and adapter trimming
- `fanse run`: FANSe alignment from FASTQ/FASTA to FANSe3 output
- `fanse sam` / `fanse bam` / `fanse bed`: FANSe3 to standard formats
- `fanse count`: read counting at gene/isoform level, including BED region counting
- `fanse quant`: merge sample count files into expression matrices and export quantification tables
- `fanse stats`: summarize FANSe logs
- `fanse cluster`: remote and distributed workflow management

## Installation

### Pip install

```bash
pip install fansetools
```

### Editable install from source

```bash
git clone https://github.com/qzhaojing/Fansetools.git
cd Fansetools
pip install -e .
```

### Conda install

After the package is published to a conda channel:

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

### Example 2 BED region counting

Count reads in selected regions using a BED file:

```bash
fanse trim -i sample.fq.gz -o sample.trim.fq.gz
fanse run -i sample.trim.fq.gz -r reference.fasta -o sample.fanse3
fanse count -i sample.fanse3 -o counts_dir --bed target_regions.bed
```

### Example 3 Multi-sample matrix aggregation

Aggregate multiple sample count files into a matrix for downstream analysis:

```bash
fanse quant -i counts_dir -p isoform_unique -c multi_to_isoform -q none -f none
```

## Release notes

Current target release: v1.2.0
