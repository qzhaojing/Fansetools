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
fanse [subcommand] -h
```
### run the alignment with fanse
fanse run -h

- **FANSe3 run as single file or batch run**
if you want to align fastq to ref, first you should use 'fanse run --set-path /path or /path/fanse.exe' to assign the position of fanse3 exe file.
```
fanse run -i sample.fastq(or *.fq,*.fq.gz,*.fastq.gz) -r ref.fasta    +fanse参数
fanse run -i sample.fastq(or *.fq,*.fq.gz,*.fastq.gz) -r ref.fasta -o c:\sample    +fanse参数 
fanse run -i sample.fastq(or *.fq,*.fq.gz,*.fastq.gz) -r ref.fasta      [ -E5  -H1  -S8  --all等所有fanse3所带参数均可附加]
```
Batch run,  you need not any change in command line, just pass the folder to the '-i' , and make sure the folder contain *.fastq or *.fq,*.fq.gz,*.fastq.gz in it. Multi folders or files are also supported.
```
fanse run -i /path1 -r ref.fasta -o c:\sample    +fanse参数

#Multi folder
fanse run -i /path1,/path2,/path3 -r ref.fasta -o c:\sample    +fanse参数
```
参数- 断点续跑
- 当由于文件原因或服务器死机等原因导致失败，可以参考日志删去最后一组不完整的文件。然后重新运行命令，加上这个参数。
自动识别文件夹中已经存在的结果文件跳过，找到没有结果生成的fastq文件继续跑。省去重复跑或者编写命令的烦恼。

- 想办法实现fanse3读取gz, fq.gz等格式，避免解压缩的繁复操作，且节省空间。可以探索fanse3能否接受标准输入，那么就可以通过python gzip模块解压缩gz，然后传给fanse3,进行比对了

### Format Conversions

- **FANSe3 to SAM:**
usage: fanse sam [-h] -i INPUT_FILE -r FASTA_FILE [-o OUTPUT]
  ```bash
  fanse sam -i input.fanse3 -r reference.fasta [-o out.sam] > output.sam
  # Or directly to sorted BAM:
  fanse sam -i input.fanse3 -r reference.fasta | samtools sort -o output.sort.bam
  ```
  > **Note:** Ensure your FANSe3 output is generated with the `--alignment` option enabled.

- **FANSe3 to BAM:**  need samtools
  - Single-end:
    ```bash
    fanse2bam -s input.fanse3 -o out.bam
    ```
  - Paired-end:
    ```bash
    fanse2bam -b input1.fanse3 input2.fanse3 -o out.bam
    ```

- **SAM/BAM to FANSe3:**
  ```bash
  fanse convert -i input.sam -o output.fanse3
  ```

- **FANSe3 to BED:**
usage: fanse bed [-h] -i INPUT [-o OUTPUT] [-n MAX_READS] [-p PATTERN]
  ```bash
  fanse bed -i input.fanse3 -o output.bed
  
  # Output can be used with bedtools and other downstream tools
  ```

- **FANSe3 to FASTQ:**
  ```bash
  fanse2fastq -i input.fanse3 -o output.fastq
  ```

- **Unmapped Reads to FASTQ:**
  ```bash
  unmapped2fastq -i input.fanse3 -o output_unmapped.fastq
  ```

### Read Counting
需要考虑的几个点
1. 转录本水平 counts
2. 基因水平 counts

- **Count reads per gene or transcript:**
  ```bash
  fanse2count -i input.fanse3 -o counts.txt
  # Options:
  #   --gene_level         # for gene-level counts
  #   --transcript_level   # for transcript-level counts
  ```

### RPKM Calculation

- **Calculate RPKM from count file:**
  ```bash
  fanse2rpkm -i counts.txt -r reference.fasta/-gxf *.gtf/gff -o rpkm.txt
  # Options:
  #   --gene_level
  #   --transcript_level
  ```

---

## Pipeline Support

FanseTools supports an end-to-end workflow, from FASTQ preprocessing to differential expression analysis, making it suitable for both research and industrial applications.

---

## Planned Roadmap

- **Fusion Gene Detection:** Tools for identifying gene fusions.
- **Improved Paired-End Support:** Enhanced handling of paired-end sequencing data.
- **Expanded Analytics:** From data preprocessing to visualization, aiming for a complete "data-to-insight" solution—ideal for clinical and industrial users who demand efficiency.

---

## License

[MIT License](LICENSE)

---

## Contact

For issues, suggestions, or contributions, please open an issue or pull request on [GitHub](https://github.com/qzhaojing/Fansetools).

---

---------------------------------------------------------------


# FanseTools
A toolkit for Fanse output treatment. Fanse is a high-accuracy algorithm solutions for next-generation sequencing. 

fanse output style:
details refer to 
```
https://github.com/qzhaojing/Fansetools/blob/main/Fanse3_output_style.MD
```


fanse以fastq文件输入，输出为fanse格式，方便直接阅读处理。
如需使用其他软件处理，可转为sam通用格式后使用其他兼容软件继续处理,支持通道处理。

### Install
To install  the  fansetools
use git，打开cmd（windows），或命令行（linux）
```
#拉取安装包
git clone https://github.com/qzhaojing/Fansetools.git
#进入文件夹
cd Fansetools
#安装
python setup.py install
```
安装之后，可以直接调用fansetools：

### fanse
**usage**: fanse [功能模块] -h

### Convert between styles，支持流式处理
 #### sam  convert fanse3 to sam file
 通过fanse2sam实现
 we can use 
 fanse sam -i input.fanse3 -r reference.fasta [-o out.sam] > output.sam
 fanse sam -i input.fanse3 -r reference.fasta  > samtools sort output.sort.bam

需注意，输入的fanse格式需打开--alignment 选项。
#### bam   -sb out.bam
通过fanse2bam实现
 -  -s 单端fanse文件直接转为bam
 -  -b 双端fanse文件转为bam
 -   
#### fanse    sam/bam convert to fanse style


##########################
#### parser  
格式化读取fanse3格式，为其他处理做准备

#### bed 
通过fanse2bed实现
fanse3转为bed格式，可接bedtools直接处理

#### fastq
fanse2fastq:


unmapped2fastq:



#### count  
通过fanse2count实现
直接统计输入fanse3文件中每个基因的readcounts
 ###### count gene_level
 ###### count transcript_level

#### rpkm 
用count文件计算rpkm, 需要-reference的长度，直接利用ref生成
 ###### rpkm gene_level
 ###### rpkm transcript_level

pipeline 
支持fastq到差异分析全流程


## 后续规划：

fusiongene  融合基因如何检测
双端比对支持，需要考虑多一些，看后续需求。

FanseTools可从单一格式转换工具升级为覆盖“数据预处理-分析-可视化”全流程的解决方案，尤其适合注重效率的临床研究和工业用户

windows下，研究下配置fanse3的功能，多文件夹适配等
