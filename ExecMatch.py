import sqlite3
import pandas as pd

def execute_sql(sql_query, connection):
    """
    执行SQL查询并返回结果。
    """
    try:
        return pd.read_sql_query(sql_query, connection)
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

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
    
    if pred_result is None or gold_result is None:
        # print("Error in executing one of the SQL queries.")
        return False
    
    # 提取数值部分，忽略列名
    pred_values = pred_result.values
    gold_values = gold_result.values
    
    # 宽松模式：只保留pred_result中包含gold_result中的列
    if ignore_extra_columns:
        # 通过数值比较来确定共享列，而不是列名
        common_columns = []
        for pred_col in pred_result.columns:
            for gold_col in gold_result.columns:
                # 通过比较列的值来确定是否相同
                if (set(pred_result[pred_col]) == set(gold_result[gold_col])):
                    common_columns.append(pred_col)
                    break
        
        if not common_columns:
            # print("Predicted SQL query does not contain the required columns.")
            return False
        
        # print(f"Common columns based on values: {common_columns}")  # 输出共同列，帮助调试
        # 只保留在pred_result中与gold_result匹配的列
        pred_result = pred_result[common_columns]
        pred_values = pred_result.values  # 重新获取经过筛选后的数值部分
    
    # 提取数值部分，忽略列名
    pred_values = pred_result.values
    gold_values = gold_result.values
    
    # 检查列数是否一致
    if pred_values.shape[1] != gold_values.shape[1]:
        # print("The number of columns in the predicted and gold queries do not match.")
        return False
    
    # 检查 gold_sql 中是否包含 "ORDER BY"
    ignore_order = "ORDER BY" not in gold_sql.upper()
    
    # 如果需要忽略顺序，对数据进行排序
    if ignore_order:
        # 对每列的数据进行排序，忽略列顺序
        pred_values_sorted = [tuple(sorted(pred_values[:, i])) for i in range(pred_values.shape[1])]
        gold_values_sorted = [tuple(sorted(gold_values[:, i])) for i in range(gold_values.shape[1])]
        
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
        return True  # 完全匹配，准确率为100%
    else:
        # print("The predicted SQL query is incorrect.")
        return False  # 结果不匹配，准确率为0%
    
# 示例使用
db_path = './SQL_Evaluation/dset.sqlite'  # 请替换为你的SQLite数据库路径
pred_sql = '''
SELECT 
    id_card, 
    name, 
    to_place, 
    to_place_address, 
    start_time
FROM (
    SELECT 
        id_card, 
        name, 
        to_place, 
        to_place_address, 
        start_time,
        ROW_NUMBER() OVER (
            PARTITION BY id_card 
            ORDER BY start_time DESC
        ) T114514
    FROM res_zh_gj_info
    WHERE name = '张三'
) T1919810
WHERE T114514 = 1
ORDER BY id_card ASC;
'''

gold_sql = '''
WITH T1 AS (
    SELECT 
        id_card, 
        name, 
        to_place, 
        to_place_address, 
        start_time,
        ROW_NUMBER() OVER (
            PARTITION BY id_card 
            ORDER BY start_time DESC
        ) T2
    FROM res_zh_gj_info
    WHERE name = '张三'
)
SELECT 
    id_card AS gmsfhm,
    name, 
    to_place_address, 
    to_place
FROM T1
WHERE T2 = 1;
'''

print(is_exec_match(pred_sql, gold_sql, db_path, ignore_extra_columns=False))
