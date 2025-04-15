import sqlite3
import pandas as pd
import numpy as np

def execute_sql(sql_query, connection):
    """
    执行SQL查询并返回结果。
    """
    try:
        return pd.read_sql_query(sql_query, connection)
    except Exception as e:
        return e

def is_exec_match(pred_sql, gold_sql, db_path, ignore_extra_columns=False) -> bool:
    """
    评估SQLite查询准确性，通过比较预测SQL结果和真实SQL结果。
    
    :param pred_sql: 预测的SQL查询
    :param gold_sql: 真实的SQL查询
    :param db_path: SQLite数据库路径
    :param ignore_extra_columns: 是否忽略预测结果中的额外列
    :return: 返回准确率（正确结果的比例）
    """
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    
    # 执行预测查询和真实查询
    pred_result = execute_sql(pred_sql, conn)
    gold_result = execute_sql(gold_sql, conn)
    
    # 关闭数据库连接
    conn.close()
    
    if isinstance(pred_result, Exception):
        return False, f"Error executing SQL query: {pred_result}"
    if isinstance(gold_result, Exception):
        return False, f"Gold SQL design error: {gold_result}"
    
    # 提取数值部分，忽略列名
    pred_values = pred_result.values
    gold_values = gold_result.values
    
    # 宽松模式：只保留pred_result中包含gold_result中的列
    if ignore_extra_columns:
        # 通过数值比较来确定共享列，而不是列名
        common_columns = []
        temp_gold_result_columns = list(gold_result.columns)
        for pred_col in pred_result.columns:
            for gold_col in temp_gold_result_columns:
                # 通过比较列的值来确定是否相同
                if (set(pred_result[pred_col]) == set(gold_result[gold_col])):
                    common_columns.append(pred_col)
                    temp_gold_result_columns.remove(gold_col)
                    break
        
        if not common_columns:
            # print("Predicted SQL query does not contain the required columns.")
            return False, "Predicted SQL query does not contain the required columns."
        
        print(f"Common columns based on values: {common_columns}")
        # 只保留在pred_result中与gold_result匹配的列
        pred_result = pred_result[common_columns]
        pred_values = pred_result.values  # 重新获取经过筛选后的数值部分
    
    # print(pred_values.shape[1])
    # print()
    # print(gold_values.shape[1])
    
    # 检查列数是否一致
    if pred_values.shape[1] != gold_values.shape[1]:
        # print("The number of columns in the predicted and gold queries do not match.")
        return False, "The number of columns in the predicted and gold queries do not match."
    
    # 检查 gold_sql 中是否包含 "ORDER BY"
    ignore_order = "ORDER BY" not in gold_sql.upper()
    
    # 如果需要忽略顺序，对数据进行排序
    if ignore_order:
        # 对每列的数据进行排序，忽略列顺序
        pred_values = np.where(pred_values == None, "", pred_values)
        gold_values = np.where(gold_values == None, "", gold_values)
        pred_values_sorted = sorted([tuple(col) for col in zip(*pred_values)])
        gold_values_sorted = sorted([tuple(col) for col in zip(*gold_values)])
        # fixed 不会打算每一列顺序，而是忽略列顺序
        # print("pred_values_sorted", pred_values_sorted)
        # print("gold_values_sorted", gold_values_sorted)
        # 将列数据作为集合进行比较，忽略列顺序
        pred_columns = set(pred_values_sorted)
        gold_columns = set(gold_values_sorted)
    else:
        # 如果有ORDER BY，我们直接比较列数据
        pred_columns = set(tuple(pred_values[:, i]) for i in range(pred_values.shape[1]))
        gold_columns = set(tuple(gold_values[:, i]) for i in range(gold_values.shape[1]))
    
    # 检查pred_result和gold_result中的列是否一致
    if pred_columns == gold_columns:
        # print("The predicted SQL query matches the gold query.")
        return True, None  # 完全匹配
    else:
        # print("The predicted SQL query is incorrect.")
        return False, "The predicted SQL query is incorrect."  # 结果不匹配
    
# # demo
# db_path = './database/dset.sqlite'
# pred_sql = '''
# SELECT 
#     *
# FROM 
#     res_0_clda_jdc
# WHERE 
#     hphm = '鲁A12345';
# '''

# gold_sql = '''
# SELECT 
#     hphm, 
#     hpzl, 
#     hpys, 
#     clsbdh, 
#     syr, 
#     sfzmhm, 
#     sfzmmc, 
#     clpp1, 
#     clpp2, 
#     clxh, 
#     ccdjrq, 
#     fprq, 
#     yxqz, 
#     bxzzrq, 
#     ccrq
# FROM 
#     res_0_clda_jdc
# WHERE 
#     hphm = '鲁A12345';
# '''

# print(is_exec_match(pred_sql, gold_sql, db_path, ignore_extra_columns=True))
