#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import pandas as pd

from fansetools.quant import build_length_maps, add_quant_columns


def main() -> int:
    try:
        # 构造注释：g1 的 geneEffectiveLength=0，应回退到 genelongesttxLength=1000；g2 正常为 1500
        annotation_df = pd.DataFrame({
            'geneName': ['g1','g2'],
            'genelongesttxLength': [1000, 2000],
            'geneEffectiveLength': [0, 1500],
        })

        len_map, eff_map = build_length_maps(annotation_df, level='gene')
        print('len_map:', len_map)
        print('eff_map:', eff_map)

        # 构造唯一基因计数
        unique_df = pd.DataFrame({'Gene': ['g1','g2'], 'Final_EM': [10.0, 5.0]})
        res_df = add_quant_columns(unique_df.copy(), id_col='Gene', count_cols=['Final_EM'], length_map=len_map, eff_length_map=eff_map, methods='tpm')
        print(res_df)

        # 验证：g1 的 TPM 应非零（有效长度已回退为1000）
        tpm_g1 = float(res_df.loc[res_df['Gene']=='g1','TPM_Final_EM'].values[0])
        assert tpm_g1 > 0, 'TPM 回退失败：g1 仍为 0'
        print('\n验证通过：基因TPM有效长度回退逻辑正确。')
        return 0
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

