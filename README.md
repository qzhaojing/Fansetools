# Fanse
A toolkit for Fanse output treatment. Fanse is a high-accuracy algorithm solutions for next-generation sequencing. 

fanse以fastq文件输入，输出为fanse格式，方便直接阅读处理。
如需使用其他软件处理，可转为sam通用格式后使用其他兼容软件继续处理,支持通道处理。

fanse :
 sam  convert fanse3 to sam file
 we can use 
 fanse sam -i input.fanse3 -r reference.fasta [-o out.sam] > output.sam
 fanse sam -i input.fanse3 -r reference.fasta  > samtools sort output.sort.bam

需注意，输入的fanse格式需打开--alignment 选项。

 parser  格式化读取fanse3格式，为转换做准备

 bed fanse3转为bed格式，可接bedtools直接处理

 

#后续加入其他转换模块
fanse2sam：ok

fanse2fastq:

fanse2bed:

unmapped2fastq:

双端比对，则
