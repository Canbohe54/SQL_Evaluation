import requests
import pandas as pd
from ExecMatch import is_exec_match
from EquiMatch import is_equi_match

def evalText2SQL(dataset_path, ignore_extra_columns=False):
    dataset = pd.read_csv(dataset_path)
    querys = dataset['query']
    results = []
    for query in querys:
        url = 'http://localhost:18888/api/v0/generate_sql'
        params = {
            'question': query
        }
        response = requests.get(url, params=params).json()
        print(response)
        if response['type'] == 'sql':
            results.append(response['text'])
        else:
            results.append('Vanna.AI error.')
    dataset['predict_sql'] = results

    equi_matchs = []
    exec_matchs = []
    for gold_sql, predict_sql in zip(dataset['gold_sql'], dataset['predict_sql']):
        equi_match = is_equi_match(gold_sql, predict_sql)
        exec_match = is_exec_match(pred_sql=predict_sql, gold_sql=gold_sql, db_path='/home/xuzequan/WORKSPACE/SQL_Evaluation/dset.sqlite', ignore_extra_columns=ignore_extra_columns)
        equi_matchs.append(equi_match)
        exec_matchs.append(exec_match)
    dataset['equi_match'] = equi_matchs
    dataset['exec_match'] = exec_matchs

    # 分数
    equi_match_score = sum(equi_matchs) / len(equi_matchs)
    exec_match_score = sum(exec_matchs) / len(exec_matchs)
    print(f'Equi-Match: {equi_match_score}')
    print(f'Exec-Match: {exec_match_score}')
    return dataset, equi_match_score, exec_match_score


if __name__ == '__main__':
    df,_,_ = evalText2SQL('/home/xuzequan/WORKSPACE/SQL_Evaluation/dataset.csv', ignore_extra_columns=True)
    df.to_csv('/home/xuzequan/WORKSPACE/SQL_Evaluation/dataset_predict_ignore.csv', index=False)