已mapping上的reads，基本格式如下，两行为一个read的mapping结果：
```
2	TCTGGCACGGTGAAGAGACATGAGAG  
F	NR_003287	0	3929	2
```
第一行两个字段：<名称> tab <read序列>

第二行五个字段：
- <正负链>：F代表正链，R代表负链
- <mapping参考序列的名称>
- <错配数>
- <mapping到参考序列上的起始位置(0-based)>
- <multi mapping次数>：若该read有多个最佳匹配位点（错配数相同且最少，称为multi mapped read），则该字段表示有多少个最佳匹配位点。若只有一个最佳匹配位点（称为unique mapped read），此字段为1.

最普通的情况下，若出现multi mapping，会将read mapping到所有参考序列中第一次出现的位置上。若mapping到负链上，会mapping到负链转正链的第一个位置（即最后一个位置上）。

如果加入开关：
首先，如果加入--showalign开关，则结果的第一行会有三个字段。第三个字段即为比对的结果，直观地给出哪里有错配。
例如：
```
42628	AGCAAGGACTAACCCCTATACC	.................x....
F	NM_001190470	1	142	1
```

第一行后面直接给出了序列比对的结果，倒数第5个碱基为错配。这样不用再行与参考序列比对即可知道哪里有错配。
这样做确实很方便，但代价是速度减慢约一倍，并且输出的文件量也会大一些。

然后来看看加了--unique开关的效果：
加了--unique开关之后，会输出三个文件：

此时，split4-refseq.fanse3里面只有unique mapped reads（即只有一个最佳匹配位点的reads），而有multi mapping的reads都被放到了split4-refseq-multimap.fanse3里面去了。这样仍然一次完成了所有reads的mapping任务，若应用严格需要unique mapped reads，则直接取第一个文件即可。若还需要multi mapped reads，则直接取-multimap.fanse3文件即可，无需重新mapping。

如果加了--all开关，则会将multi mapping的所有最佳匹配位点全部输出出来。若没有--showalign开关，则性能几乎没有损失。若与--showalign开关联用，则会拖慢3倍以上的速度。
输出的multi mapping结果示例：
```
369061	AGCTGGTACAGAAAGCCAAATTCGCTG	....................x......,....................x......
F,F	NM_003404,NM_139323	1	405,310	2
```
可以注意到，比对结果、正负链、参考序列、位置号 都以逗号分隔多个mapping结果。
这个read可以被mapping到两个地方，都是1个错配，都是mapping在正链上，分别mapping到NM_003404,NM_139323两个参考序列上，位置号分别为405,310。

最普通的运行方式是：
Fanse3 -RF:\1g\RefSeq\refseq.fa -DF:\1g\RefSeq\split4.fastq -L55 -E2 -S13
这将产生两个文件：

其中，split4-refseq.fanse3存放已mapping的结果，split4-refseq.unmapped存放未mapping上的reads。

未mapping上的reads按如下方式存放：
11	CAGATGAGGAAGAACCAAAAGATGATATCGTATGCC
13	CTGCTACAAGAACCCACTGCAGATGCAGC
14	GCTTAATTTGACCCAACACGG
18	GAACACAGGCAGCAGGTGCTGAGGTC
21	ATGCCACTCCTTTCCCGTGCTCCAG
25	GCTAACTTTCAAGAGGTGCAG
26	GATAGCACTATTGCACTCCAGCCT
29	CAAAGTTTGGAATAGTCAG
39	AGAAATTCAATGAAGCGCGGGTAAATTCGTATGCCG
40	TCTGGCATGTGGAACAATGT
49	GGAGGACGAGGACGAAACCTGGAG
即 <名称> tab <序列> 的方式。若没有--rename开关，则<名称>完全依照原FASTQ文件中的reads名称。
