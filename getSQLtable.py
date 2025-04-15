import re

sql = """
    SELECT 
    hphm, 
    hpzl, 
    hpys, 
    clsbdh, 
    syr, 
    sfzmhm, 
    sfzmmc, 
    clpp1, 
    clpp2, 
    clxh, 
    ccdjrq, 
    fprq, 
    yxqz, 
    bxzzrq, 
    ccrq
FROM 
    res_0_clda_jdc JOIN res_0_clda_jdc_clxh ON res_0_clda_jdc.id = res_0_clda_jdc_clxh.id
WHERE 
    hphm = '鲁A12345';
"""

def extract_table_names(sql: str):
    pattern = r'''
        (?:
            \bFROM\b\s+([`"\[]?\w+[`"\]]?) |
            \bJOIN\b\s+([`"\[]?\w+[`"\]]?) |
            \bINSERT\s+INTO\b\s+([`"\[]?\w+[`"\]]?) |
            \bREPLACE\s+INTO\b\s+([`"\[]?\w+[`"\]]?) |
            \bUPDATE\b\s+([`"\[]?\w+[`"\]]?) |
            \bDELETE\s+FROM\b\s+([`"\[]?\w+[`"\]]?) |
            \bMERGE\s+INTO\b\s+([`"\[]?\w+[`"\]]?) |
            \bUSING\b\s+([`"\[]?\w+[`"\]]?) |
            \bWITH\b\s+\w+\s+AS\s*\(\s*SELECT.*?\bFROM\b\s+([`"\[]?\w+[`"\]]?)
        )
    '''
    matches = re.findall(pattern, sql, re.IGNORECASE | re.VERBOSE | re.DOTALL)
    tables = [t for tup in matches for t in tup if t]
    return sorted(set(tables))  # 去重 + 排序

# 提取表名
tables = extract_table_names(sql)
print(tables)
