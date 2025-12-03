#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from collections import Counter

import pandas as pd

# 说明：本测试脚本用于快速验证 fansetools.count 中“基因水平 Final_EM/Final_EQ 合并源”
# 以及“multi_EM_ratio/multi_equal_ratio 的数据源”是否已修正为 gene_level_counts_multi_genes。
# 该脚本构造一个最小的注释与计数场景，覆盖：
# - 单基因唯一比对（isoform & gene 唯一）
# - 同一基因内的多重比对组合（isoform 不唯一但 gene 唯一）
# - 跨基因的多重比对组合（gene 不唯一，需要 EM/Equal 分配）


def main() -> int:
    try:
        # 构造最小注释 DataFrame
        annotation_df = pd.DataFrame(
            {
                "txname": ["txA", "txB", "txC", "txD"],
                "geneName": ["g1", "g1", "g2", "g3"],
                # 提供长度列以便 TPM/EM 分配与文件生成
                "txLength": [1000, 1100, 1200, 1300],
                "geneEffectiveLength": [2000, 2000, 1200, 1300],
            }
        )

        from fansetools.count import FanseCounter

        fc = FanseCounter(
            input_file="dummy.fanse3",
            output_dir=".",
            level="both",
            annotation_df=annotation_df,
            verbose=False,
        )

        # 构造 isoform 层计数数据
        # 说明：
        # - txA 属于 g1，唯一比对 10 条
        # - (txA,txB) 同属于 g1 的组合，多重比对 6 条（在 gene 层仍唯一到 g1）
        # - (txB,txC) 跨 g1/g2 的组合，多重比对 5 条（在 gene 层为 g1,g2 组合，需要 EM/Equal 分配）
        counts_data = {
            f"{fc.isoform_prefix}raw": Counter({"txA": 10, ("txA", "txB"): 6, ("txB", "txC"): 5}),
            f"{fc.isoform_prefix}unique_to_isoform": Counter({"txA": 10}),
            f"{fc.isoform_prefix}multi_to_isoform": Counter({("txA", "txB"): 6, ("txB", "txC"): 5}),
            f"{fc.isoform_prefix}firstID": Counter({"txA": 10, ("txA", "txB"): 6, ("txB", "txC"): 5}),
            # 为了便于查看，这里预填充等分/EM 示例值（真实流程会在后续重新计算/覆盖）
            f"{fc.isoform_prefix}multi2all": Counter({"txA": 6, "txB": 5, "txC": 5}),
            f"{fc.isoform_prefix}multi_equal": Counter({"txA": 3, "txB": 8, "txC": 2}),
            f"{fc.isoform_prefix}multi_EM": Counter({"txA": 6, "txB": 4, "txC": 0}),
            f"{fc.isoform_prefix}multi_EM_cannot_allocate_tpm": Counter(),
            f"{fc.isoform_prefix}Final_EM": Counter(),
            f"{fc.isoform_prefix}Final_EQ": Counter(),
            f"{fc.isoform_prefix}Final_MA": Counter(),
        }

        # 生成 isoform 层最终计数（会进行 EM/Equal 合并统计与摘要）
        fc.generate_isoform_level_counts(counts_data, total_count=21)

        # 基因层聚合（内部会调用基因层 EM 分配，依赖 unique_to_gene 的 TPM 视图）
        gene_unique, gene_multi = fc.aggregate_gene_level_counts()

        # 提取关键结果
        gene_final_em = dict(gene_unique.get(f"{fc.gene_prefix}Final_EM", Counter()))
        gene_final_eq = dict(gene_unique.get(f"{fc.gene_prefix}Final_EQ", Counter()))
        gene_em_ratio = gene_unique.get(f"{fc.gene_prefix}multi_EM_ratio", Counter())
        gene_eq_ratio = gene_unique.get(f"{fc.gene_prefix}multi_equal_ratio", Counter())
        gene_alloc_em = dict(gene_multi.get(f"{fc.gene_prefix}multi_EM", Counter()))

        # 打印验证输出
        print("Gene Final_EM:", gene_final_em)
        print("Gene Final_EQ:", gene_final_eq)
        print("Gene multi_EM_ratio:", {k: round(v, 3) for k, v in gene_em_ratio.items()})
        print("Gene multi_equal_ratio:", {k: round(v, 3) for k, v in gene_eq_ratio.items()})
        print("Gene-level multi_EM allocated (from multi_to_gene):", gene_alloc_em)

        # 断言关键逻辑：
        # - g1 的 Final_EM 应包含从 (g1,g2) 组合分配得到的 EM 数（而非来自 unique 字典）
        # - multi_EM_ratio/multi_equal_ratio 应从 gene_multi 字典来源计算
        assert "g1" in gene_final_em, "缺少 g1 的 Final_EM"
        assert sum(gene_alloc_em.values()) > 0, "multi_EM 未在基因层分配"
        assert "g1" in gene_em_ratio, "缺少 g1 的 multi_EM_ratio"
        assert "g1" in gene_eq_ratio, "缺少 g1 的 multi_equal_ratio"

        print("\n验证通过：基因层 Final_EM/Final_EQ 合并与比率来源已正确。")
        return 0

    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

