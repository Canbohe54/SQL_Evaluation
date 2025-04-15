import requests
import pandas as pd
from ExecMatch_with_Err import is_exec_match
from EquiMatch_with_Err import is_equi_match

def evalText2SQL(dataset_path, db_path, ignore_extra_columns=False):
    dataset = pd.read_csv(dataset_path)
    querys = dataset['query']
    results = []
    for query in querys:
        url = 'http://localhost:8084/api/v0/generate_sql'
        params = {
            'question': query
        }
        response = requests.get(url, params=params).json()
        print(response)
        if response['type'] == 'sql':
            results.append(response['text'])
        else:
            results.append('Vanna.AI error, response text: ' + response['text'])
    dataset['pred_sql'] = results

    equi_golds = []
    equi_preds = []
    equi_matchs = []
    equi_msgs = []
    exec_matchs = []
    exec_msgs = []
    for gold_sql, predict_sql in zip(dataset['gold_sql'], dataset['pred_sql']):
        equi_gold, equi_pred, equi_match, equi_msg = is_equi_match(gold_sql, predict_sql)
        exec_match, exec_msg = is_exec_match(pred_sql=predict_sql, gold_sql=gold_sql, db_path=db_path, ignore_extra_columns=ignore_extra_columns)
        equi_golds.append(equi_gold)
        equi_preds.append(equi_pred)
        equi_matchs.append(equi_match)
        equi_msgs.append(equi_msg)
        exec_matchs.append(exec_match)
        exec_msgs.append(exec_msg)
    dataset['equi_gold'] = equi_golds
    dataset['equi_pred'] = equi_preds
    dataset['equi_match'] = equi_matchs
    dataset['equi_msg'] = equi_msgs
    dataset['exec_match'] = exec_matchs
    dataset['exec_msg'] = exec_msgs

    # 分数
    equi_match_score = sum(equi_matchs) / len(equi_matchs)
    exec_match_score = sum(exec_matchs) / len(exec_matchs)
    print(f'Equi-Match: {equi_match_score}')
    print(f'Exec-Match: {exec_match_score}')
    return dataset, equi_match_score, exec_match_score


if __name__ == '__main__':
    df,_,_ = evalText2SQL('/data1/WORKSPACE/SQL_Evaluation/dataset.csv', '/data1/WORKSPACE/database/dset.sqlite', ignore_extra_columns=True)
    df.to_csv('/data1/WORKSPACE/SQL_Evaluation/dataset_predict_with_error.csv', index=False)