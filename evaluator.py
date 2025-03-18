import sqlite3
import sqlparse
def eval_execute_match(sqlite_db,pred_sql,gold_sql):
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()
    try:
        cursor.execute(pred_sql)
        pred_res = cursor.fetchall()
    except Exception as e:
        return 0
    cursor.execute(gold_sql)
    gold_res = cursor.fetchall()
    if len(pred_res) != len(gold_res): # 这个len是row的数量，若查询结果行数不同，直接返回0
        # TODO 若数据量太大，截断处理的判断逻辑
        return 0
    
    return