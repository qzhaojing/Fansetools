# FANSeTools 官方文档

## 1. 总体介绍 (Introduction)

**FANSeTools** 是专为 [FANSe3](http://bioinformatics.jnu.edu.cn/FANSe3/) 超高速高精度序列比对算法设计的综合性配套工具集。它旨在弥补 FANSe3 在文件格式兼容性、后续数据分析、集群分布式计算以及流程自动化方面的空白，为生物信息学研究人员提供一站式的 NGS 数据处理解决方案。

### 核心特点 (Key Features)

*   **一站式流程**: 从原始数据质控 (`trim`)、序列比对 (`run`)、定量计数 (`count`) 到格式转换 (`sam`/`bam`) 全覆盖。
*   **分布式计算支持**: 内置强大的集群管理模块 (`cluster`)，支持多节点并行计算，自动分发任务与数据，极大提升大规模样本处理效率。
*   **格式全面兼容**: 完美支持 FANSe3 原生格式与通用生物信息学格式 (SAM/BAM/BED/Mpileup) 之间的无缝转换。
*   **智能便捷**: 大量自动化参数推断，支持通配符批量处理，内置 GZIP 压缩支持，简化命令行操作。
*   **模块化扩展**: 提供包管理器 (`install`)，可轻松扩展第三方工具 (如 samtools, fastp 等) 和自定义流程 (`flow`)。

### 应用场景 (Application Scenarios)

1.  **RNA-seq 定量分析**: 快速完成从 Fastq 到 Gene/Isoform 表达量矩阵的计算。
2.  **基因组学分析**: 将 FANSe3 的高精度比对结果转换为 BAM 文件，用于变异检测 (SNP/Indel) 或可视化 (IGV)。
3.  **大规模数据清洗**: 利用集群模式快速对海量测序数据进行质控和预处理。
4.  **云端/集群部署**: 在多台服务器组成的集群环境中快速部署和调度比对任务。

### 拓展与未来 (Future)

FANSeTools 正在向着更通用的生物信息学流程管理平台发展。未来将加强对单细胞测序数据的支持，集成更多下游分析流程 (如差异表达分析、富集分析)，并进一步优化分布式调度算法，支持容器化 (Docker/Singularity) 运行环境。

---

## 2. 模块详解 (Modules)

### 2.1 核心运行模块: fanse run

**简介**: 批量运行 FANSe3 比对任务的核心工具。支持单端/双端测序数据，支持 GZIP 压缩输入，具备本地并行和集群分发能力。

**用法**:
```bash
fanse run -i <输入文件/目录> -r <参考序列> -o <输出目录> [选项]
```

**简单示例**:
```bash
# 单个文件运行
fanse run -i sample.fq.gz -r ref.fasta -o output_dir/

# 批量运行目录下的所有文件
fanse run -i /data/fastq/*.fq.gz -r ref.fasta -o output_dir/

# 集群模式运行 (自动分发到配置的节点)
fanse run --cluster -i *.fq.gz -r ref.fasta -o output_dir/
```

**详细参数说明**:

| 参数 | 说明 |
| :--- | :--- |
| `-i, --input` | 输入文件或目录。支持通配符 (如 `*.fq.gz`)，多个路径用分号隔开。支持 `.gz` 自动解压。 |
| `-r, --refseq` | 参考序列文件路径 (FASTA 格式)。 |
| `-o, --output` | 输出目录或文件路径。若指定目录，会自动根据输入文件名生成输出文件名。 |
| `-w, --work-dir` | 临时工作目录，用于存放解压后的临时文件 (默认系统临时目录)。 |
| `--cluster` | **开启集群模式**。将任务分发到 `fanse cluster` 配置的节点上运行。 |
| `-n, --nodes` | (集群模式) 指定运行的节点名称，用逗号分隔。默认使用所有可用节点。 |
| `--timeout` | (集群模式) 任务超时时间。 |
| `-C, --cores` | 本地运行时使用的并行核数 (默认: CPU核数-2)。 |
| `-L` | FANSe3 参数: 最大读长 (默认: 1000)。 |
| `-E` | FANSe3 参数: 允许的错配数 (默认: 5)。 |
| `-S` | FANSe3 参数: Seed 长度 (默认: 13)。 |
| `-I` | FANSe3 参数: Indel 检测 (0=关闭, 1=开启)。 |
| `--unique` | 仅输出唯一比对结果。 |
| `--debug` | 调试模式，仅打印命令而不执行。 |

---

### 2.2 定量计数模块: fanse count

**简介**: 基于 FANSe3 的比对结果进行表达量定量。支持 Isoform 和 Gene 水平的计数，可计算 RPKM/TPM，支持多重比对 reads 的智能分配。

**用法**:
```bash
fanse count -i <输入文件> -o <输出目录> --gxf <注释文件> [选项]
```

**简单示例**:
```bash
# 基本定量 (默认 Isoform 水平)
fanse count -i sample.fanse3 -o results/ --gxf genome.gtf

# 基因水平定量，并计算 TPM
fanse count -i *.fanse3 -o results/ --gxf genome.gtf --level gene --tpm
```

**详细参数说明**:

| 参数 | 说明 |
| :--- | :--- |
| `-i, --input` | 输入的 FANSe3 格式文件 (支持通配符批量处理)。 |
| `-o, --output` | 结果输出目录。 |
| `--gxf` | 基因组注释文件 (GTF/GFF3)，用于基因水平归并。 |
| `--level` | 定量水平: `isoform` (默认) 或 `gene`。 |
| `--tpm` | 计算并输出 TPM (Transcripts Per Million) 值。 |
| `--rpkm` | 计算并输出 RPKM 值。 |
| `--multi-strategy` | 多重比对 reads 分配策略: `unique` (丢弃), `uniform` (平均分配), `prop` (按比例分配)。 |
| `-p, --threads` | 并行处理的线程数。 |

---

### 2.3 汇总导出模块: fanse quant

**简介**: 将 `fanse count` 生成的样本级 unique 计数 CSV 汇总为表达矩阵，并可选导出为 RSEM / Salmon / Kallisto / featureCounts 等主流定量工具的标准格式，便于无缝对接下游差异表达分析（DESeq2、edgeR）流程。

**用法**:
```bash
fanse quant -i <输入文件/目录> -p <文件类型> [选项]
```

**简单示例**:
```bash
# 完整模式: 合并 isoform unique CSV + 计算 TPM + 导出 RSEM 格式文件
fanse quant -i results/*.counts_isoform_level_unique.csv -a genome.refflat -p isoform_unique -l isoform -o quant_out/ -m matrix.csv

# 纯矩阵模式: 仅合并 unique 计数矩阵，无需注释文件与输出目录
fanse quant -i results/ -p isoform_unique -c unique -q none -f none -m isoform_unique_matrix.csv

# 基因水平矩阵并附带 TPM/RPKM
fanse quant -i results/ -p gene_unique -a genome.refflat -c Final_EM -q both -f none -m gene_matrix.csv
```

**详细参数说明**:

| 参数 | 说明 |
| :--- | :--- |
| `-i, --inputs` | 输入文件或目录（支持通配符/逗号分隔）。gene: `*.counts_gene_level_unique.csv`；isoform: `*.counts_isoform_level_unique.csv`。 |
| `-a, --annotation` | 注释文件（`.refflat`/`.gtf`/`.gff`/`.gff3`），用于构建长度映射。**条件必选**: 当 `-q` 非 `none` 或 `-f` 非 `none` 时必须提供；纯计数矩阵模式（`-q none -f none`）可省略。 |
| `-p, --pattern` | 输入文件类型: `gene_unique`（默认）或 `isoform_unique`。 |
| `-l, --level` | 导出层级: `gene` / `isoform` / `both`（默认 `gene`；gene_unique 输入时 isoform 无效）。 |
| `-f, --format` | 导出格式: `rsem`（默认）/ `salmon` / `kallisto` / `featureCounts` / `all` / `none`。`none` 表示不导出格式文件、仅输出合并矩阵（此时 `--annotation` 可省略）。 |
| `-o, --outdir` | **统一输出参数**: 给目录 → 格式文件与矩阵输出到该目录（矩阵名 `<pattern>_matrix.csv`）；给 `.csv` 文件路径 → 直接作为矩阵输出路径（格式文件输出到其所在目录）；未指定时矩阵输出到 `-i` 输入数据文件夹下；仅给文件名（无路径）时解析到 `-i` 文件夹下。**条件必选**: `-f` 非 `none` 时必须提供。 |
| `-c, --count-type` | 计数列选择: 支持任意 `fanse count` 输出的列名（如 `raw` / `unique_to_isoform` / `unique_to_gene` / `multi_to_isoform` / `multi_to_gene` / `multi2all` / `multi_equal` / `firstID` / `Final_EM`（默认）/ `Final_EQ` / `Final_MA` 等），逗号分隔多列用于格式导出（矩阵取第一列）。`unique`/`multi` 为智能别名: 按 `-p` 自动映射为 `*_to_isoform`（isoform 文件）或 `*_to_gene`（gene 文件）；`all` 输出全部主要列；列名错误时运行时列出实际可用列。 |
| `-q, --quant` | 矩阵定量类型: `none`（仅计数）/ `tpm`（默认）/ `rpkm` / `both`。 |

**输出说明**:
*   **矩阵**（默认输出）: 所有样本的 ID 取并集并排序，缺失值补 `0`；计数列命名为 `<样本名>_count`，TPM/RPKM 列命名为 `<样本名>_tpm` / `<样本名>_rpkm`。**全零/全空行自动剔除**: 所有样本中均无计数的 ID 不会输出。未指定 `-o` 时输出到 `-i` 数据文件夹下。
*   **样本名**: 输入文件名去除 `.counts_gene_level_unique.csv` / `.counts_isoform_level_unique.csv` 后缀。
*   **格式文件**: 按 `-f` 指定格式输出到 `-o` 目录，文件名形如 `<样本名>.rsem.genes.results`、`<样本名>.salmon.genes.sf` 等。

---

### 2.4 集群管理模块: fanse cluster

**简介**: 管理分布式计算集群。可以将多台服务器 (Windows/Linux) 组成一个计算资源池，统一调度任务。

**用法**:
```bash
fanse cluster <子命令> [参数]
```

**子命令**:
*   `add`: 添加节点
*   `list`: 列出节点
*   `check`: 检查节点状态
*   `run`: 在节点上执行命令
*   `remove`: 移除节点

**简单示例**:
```bash
# 添加一个节点
fanse cluster add node1 192.168.1.100 user /path/to/fanse3

# 查看所有节点
fanse cluster list

# 检查连接状态
fanse cluster check
```

**详细参数说明**:
*   `add <name> <host> <user> [fanse_path]`: 添加节点。需配置 SSH 免密登录或密码。
*   `list`: 显示节点列表及其状态、最大任务数等配置。
*   `run -n <node> <command>`: 在指定节点执行任意 Shell 命令。

---

### 2.4 质控与修剪模块: fanse trim

**简介**: 自动化的测序数据质控与接头去除工具。底层集成 `fastp` (推荐) 和 `cutadapt`，提供“无脑”的一键化操作体验。

**用法**:
```bash
fanse trim -i <输入文件> [选项]
```

**简单示例**:
```bash
# 自动检测接头并过滤 (单端)
fanse trim -i input.fq.gz

# 双端数据处理
fanse trim -1 R1.fq.gz -2 R2.fq.gz -p 8
```

**详细参数说明**:
| 参数 | 说明 |
| :--- | :--- |
| `-i, --input` | 单端输入文件。 |
| `-1 / -2` | 双端输入文件 (R1/R2)。 |
| `-o / -O` | 输出文件路径 (可选，默认自动生成)。 |
| `-a, --adapter` | 接头序列 (可选，不指定则自动检测)。 |
| `-q, --quality` | 质量修剪阈值 (Phred score)。 |
| `-l, --length` | 最小长度过滤。 |
| `--fastp` / `--cutadapt` | 强制指定使用的底层工具。 |

---

### 2.5 格式转换模块: fanse bam / sam / bed

**简介**: 将 FANSe3 的私有格式转换为通用的生物信息学格式。

*   **fanse sam**: 转换为 SAM 格式 (Sequence Alignment/Map)。
*   **fanse bam**: 转换为 BAM 格式 (Binary SAM)，更节省空间，需安装 `samtools`。
*   **fanse bed**: 转换为 BED 格式，用于查看基因组覆盖情况。

**用法**:
```bash
fanse sam -i input.fanse3 -r ref.fasta -o output.sam
fanse bam -i input.fanse3 -r ref.fasta -o output.bam
```

**详细参数说明**:
| 参数 | 说明 |
| :--- | :--- |
| `-i, --input` | 输入 FANSe3 文件。 |
| `-r, --ref` | 参考序列文件 (转换 SAM/BAM 必需，用于填充 Header)。 |
| `-o, --output` | 输出文件路径。 |
| `--sort` | (SAM/BAM) 输出排序方式: `coord` (坐标) 或 `name` (名称)。 |
| `-R, --region` | (SAM) 仅输出特定区域的结果 (如 `chr1:1000-2000`)。 |

---

### 2.6 其他实用工具

#### fanse fastx
FASTQ/FASTA 序列处理工具。
*   **功能**: 格式互转 (`--fasta2fastq`, `--fastq2fasta`)，提取未比对序列 (`--unmapped`)。
*   **示例**: `fanse fastx -i input.fa --fasta2fastq -o output.fq`

#### fanse mpileup
生成 mpileup 格式文件，用于变异检测。
*   **用法**: `fanse mpileup input.fanse3 reference.fasta -o out.mpileup`

#### fanse sort
对 SAM 文件进行排序。
*   **用法**: `fanse sort -i input.sam -o sorted.sam --coord`

#### fanse install / list
包管理器，用于安装 FANSeTools 依赖的外部工具 (如 samtools, bwa 等)。
*   **列出可用包**: `fanse list`
*   **安装包**: `fanse install samtools`

#### fanse parser
解析 FANSe3 文件并输出结构化数据，主要用于开发者调试或自定义脚本处理。

#### fanse flow
运行预定义的分析流程脚本。

#### fanse path / test
路径解析测试与功能自检工具，用于排查环境问题。

---
*文档生成时间: 2025-12-11；最近更新: 2026-09-04（新增 2.3 fanse quant 模块文档）*
