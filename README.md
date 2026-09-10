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

### Example 4 FANSe3 → BAM conversion with PE fixmate

Convert FANSe3 output directly to sorted, indexed BAM. For paired-end (PE) data, the pipeline runs a full `queryname sort → fixmate → coordinate sort` chain to restore mate pair information:

```bash
# Single-end (SE) — simple pipe: fanse sam → view → sort (coord)
fanse bam -i sample.fanse3 -r reference.fa -o sample.bam

# Paired-end (PE) — automatically discovers R1/R2 and runs fixmate
fanse bam -i sample_R1.fanse3 -r reference.fa -o sample_pe.bam --pe

# Network I/O optimization for UNC paths
fanse bam -i \\fs2\d\data\sample.fanse3 -r \\fs2\d\ref.fa -o \\fs2\d\out\sample.bam \
    --preload --local
```

#### Input forms for `-i`

`-i` accepts five forms (extensions `.fanse3` / `.fanse` / `.sam`):

| Form | Example | Behavior |
|------|---------|----------|
| Single file | `-i sample.fanse3` | Convert one file |
| Comma-separated list | `-i a.fanse3,b.fanse3` | Convert each file in order |
| Wildcard | `-i "dir\*_R1_*.fanse3"` | Glob match (supports recursive `**`); on Windows `/` is normalized to `\` |
| Directory | `-i dir` | Collect **all** valid files directly inside the directory (one level, non-recursive) |
| Mixed | `-i dir,a.fanse3,"b*.fanse3"` | Comma-separated mix of the above |

##### Directory mode and PE pairing

With `--pe`, directory mode (`-i dir`) and wildcards now **auto-pair** R1/R2: every input file matching the R1 naming convention auto-discovers its R2 partner (see below), and any R2 file already covered by an R1's pairing is skipped automatically (`跳过 xxx: 已由其 R1 配对文件以 --pe 模式合并转换`). So `-i dir --pe` converts each PE library exactly once — R1+R2 merged into one BAM per library:

```bash
# Whole folder of PE libraries — one BAM per library
fanse bam -i dir -r ref.fa -o bam_out/ --pe

# Equivalent explicit form
fanse bam -i "dir\*_R1_*.fanse3" -r ref.fa -o bam_out/ --pe
```

Notes: R2-only folders (no R1 counterpart) still convert each R2 as an independent input; the pair-consistency check runs per discovered pair as usual (`--force-pair` skips).

##### Automatic R2 discovery (`--pe`)

When `--pe` is set, giving the R1 file alone is enough — the matching R2 `fanse3` and its `.unmapped` are auto-discovered in the same directory by these name patterns (first match wins):

| R1 input stem | Auto-discovered R2 stem |
|---------------|------------------------|
| `CQ304891_S16_L001_R1_001` (Illumina/BGI standard) | `CQ304891_S16_L001_R2_001` |
| `sample_R1.fanse3` / `sample-R1-x` | `sample_R2.fanse3` / `sample-R2-x` (`_R1_`, `_R1.`, `-R1-`, `-R1.`, `_R1`, `-R1`) |
| `sample_1.fanse3` / `sample-1.fanse3` | `sample_2.fanse3` / `sample-2.fanse3` (`_1.`, `_1_`, `-1.`, `_1`) |
| any stem containing `R1` | direct `R1` → `R2` replacement (fallback) |

Unmapped companions are discovered as `<stem>.unmapped` next to each fanse3 file. After R2 is found, a flowcell-set + QNAME sampling consistency check runs automatically (`--force-pair` skips it). If no R2 is found, a warning is printed and only R1 is converted (fixmate will have no mates to repair).

##### Bare base name (without R1/R2 markers) — not auto-expanded

`-i sample.fanse3` will **not** auto-expand to `sample_R1.fanse3` + `sample_R2.fanse3`. Discovery only rewrites an existing R1/`_1` marker to R2/`_2`. To convert a pair from a bare base name, use a wildcard:

```bash
fanse bam -i "sample*R[12]*.fanse3" -r ref.fa -o bam_out/ --pe
```

#### Pipeline architecture (PE + `--preload` + `--local`)

```
  [UNC network] fanse3 + unmapped (R1/R2)  ── --preload ──▶  [local temp] 预加载副本
                                                                │
  [UNC network] ref.fa (.ref_info.json cached)                  │
                                                                ▼
                                                         fanse sam (单线程, -t n)
                                                          │
                                                          ▼ stdout pipe
                                                    samtools view -bS
                                                          │
                                                          ▼ stdout pipe
  ┌─── 全部在本地 workspace ──────────────────────────────────┤
  │                                                          ▼
  │                                             samtools sort -n (queryname)
  │                                             -T <workspace>/sort_name
  │                                                          │
  │                                                          ▼
  │                                             samtools fixmate -m
  │                                                          │
  │                                                          ▼ stdout pipe
  │                                             samtools sort (coordinate)
  │                                             -T <workspace>/sort_coord
  │                                                          │
  │                                                          ▼
  │                                             samtools index → .bai
  └───────────────────────────────────────────────────────────┘
                                                                │
                                                          校验通过
                                                                ▼
  [UNC network]  sample.bam.copying ── os.replace ──▶  sample.bam
                sample.bai.copying   ── os.replace ──▶  sample.bai
                *.bam_conv.log       ── 最终拷回   ──▶  *.bam_conv.log
```

#### Key parameters

| Flag | Description |
|------|-------------|
| `--pe` | Enable paired-end mode. Automatically discovers matching `*_R2.fanse3` files and runs `fixmate` after queryname sort. |
| `--preload` | Pre-copy UNC network-drive inputs (`fanse3`, `unmapped`) to a local temp directory before starting `fanse sam`. Avoids network read latency during record parsing. |
| `--local [PATH]` | Route all BAM intermediate I/O (sort temp blocks, `fixmate` output, final BAM) through a dedicated local workspace. Without PATH, uses `%TEMP%\fansetools_temp`. With PATH, creates subdirs under the given directory. Final BAM/BAI are copied to the original `-o` target **only after** validation and indexing succeed. |
| `--force-pair` | Skip the R1/R2 flowcell consistency check. Use only when you have verified the paired files are genuinely from the same sequencing run. |
| `--no-sort` / `--no-index` | Skip coordinate sort or BAI indexing. |

#### conv_log format

Every conversion writes a structured log to `<output>.bam_conv.log` with wall-clock timestamps and per-stage elapsed time:

```
[2026-09-10 14:30:22] === fanse2bam_win_pipe 启动 ===
[2026-09-10 14:30:22] 输入: \\fs2\d\data\sample.fanse3
[2026-09-10 14:30:22] PE=True, sort=True, index=True, workspace=C:\...\fansetools_temp\bam_xxxx
[2026-09-10 14:30:22] conv_log 本地缓冲: C:\...\bam_xxxx\sample.bam_conv.log 目标路径: \\fs2\d\data\sample.bam_conv.log
[2026-09-10 14:30:22] [PE] fanse sam → view → sort -n 开始
... tqdm progress from fanse sam stderr ...
[2026-09-10 16:15:11] [PE] fanse sam → view → sort -n 结束 (耗时 689.2s, fanse_rc=0, view_rc=0, sort_n_rc=0)
[2026-09-10 16:15:11] [PE] fixmate → coordinate sort 开始
[2026-09-10 16:16:02] [PE] fixmate → coordinate sort 结束
[2026-09-10 16:16:02] BAM 校验 → 索引 → 复制到目标盘 开始
[2026-09-10 16:16:05] BAM 校验 → 索引 → 复制 结束 (耗时 3.2s, BAM=1234.5MB, BAI=85.2MB)
[2026-09-10 16:16:05] === fanse2bam_win_pipe 成功完成 ===
```

#### Performance notes

The pipeline is **CPU-bound inside `fanse sam`**, not I/O-bound:

- `fanse3 → SAM` record conversion is multi-threaded Python ( `-t n` should be carefully seted to avoid multi-process pickle memory explosions on heavy multi-mapping files). At ~30k reads/s(1.8M/min) when `-t 4`, a 58.7M-record file takes ~20 min regardless of local vs network I/O.
- `samtools view/sort/fixmate` are **downstream consumers** of the pipe — they do not block `fanse sam`; they immediately process whatever the producer emits. Moving sort temp blocks to a local workspace (`--local`) only affects samtools write I/O, which is a fraction of total wall time.
- `--preload` removes network **read** latency for the FANSe3 input files, but the parsing CPU cost remains.
- **Observed bottleneck**: Python-side record parsing (CIGAR decoding, SA tag construction, SAM line assembly). Future optimizations:
  1. PE R1/R2 `fanse sam` in parallel processes (currently fully serial; R1 finishes before R2 starts).
  2. Cython/numba hot-path for `fanse_parser_high_performance`.
  3. Skip full SAM serialization — direct FANSe3 → BAM writer.

## Release notes

Current target release: v1.2.0
