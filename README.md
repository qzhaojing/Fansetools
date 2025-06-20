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
usage: fanse **
### Convert between styles，支持流式处理
 #### sam  convert fanse3 to sam file
 we can use 
 fanse sam -i input.fanse3 -r reference.fasta [-o out.sam] > output.sam
 fanse sam -i input.fanse3 -r reference.fasta  > samtools sort output.sort.bam

需注意，输入的fanse格式需打开--alignment 选项。

#### fanse    sam/bam convert to fanse style


#### parser  
格式化读取fanse3格式，为其他处理做准备

#### bed 
fanse3转为bed格式，可接bedtools直接处理

#### count  
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
加入其他转换模块
fanse2sam：ok

fanse2fastq:

fanse2bed:

unmapped2fastq:


fusiongene  融合基因如何检测
双端比对支持，需要考虑多一些，看后续需求。
FanseTools可从单一格式转换工具升级为覆盖“数据预处理-分析-可视化”全流程的解决方案，尤其适合注重效率的临床研究和工业用户

