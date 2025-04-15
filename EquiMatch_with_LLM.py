from openai import OpenAI
import re
import json
def is_equi_match_with_llm(sql1: str, sql2: str, base_url, api_key, model_name, temperature=0.7):
    client = OpenAI(
    base_url=base_url,
    api_key=api_key,
)
    prompt = f"""
请判断如下两个SQL查询语句是否等价。在判断 SQL 查询语句等价性时，请使用语义等价而非严格语法等价的标准。两个 SQL 查询如果满足以下条件，应视为等价：

1. 核心数据关系相同：相同的表连接和连接条件，无论表别名如何不同
2. 查询意图相同：即使列名、列顺序或列别名不同，只要能满足查询的基本信息需求
3. 以下差异可以忽略：
   - 表别名差异 
   - 列别名差异
   - 列的呈现顺序差异
   - 查询包含原始查询的核心列的子集

4. 以下情况仍视为不等价：
   - 不同的过滤条件 (WHERE 子句不同)
   - 不同的连接类型 (内连接、左连接等)
   - 对结果集的本质操作不同 (如聚合、分组差异)

总之，判断两个查询时，请关注它们是否提取相同的基础数据，而非完全相同的结果格式。

请先进行简单的分析，并以如下的JSON格式给我结果：

```json
{{
   "result": 布尔类型，true表示等价，false表示不等价
   "reason": 字符串，概述你的判断依据
}}
```

下面是你需要判断的两个SQL语句：

```sql
{sql1}
```

```sql
{sql2}
```
    """
    
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=114514,
    )
    json_pattern = r"```json([\s\S]*?)```"
    result = re.search(json_pattern, response.choices[0].message.content).group(1)
    result_dict = json.loads(result)
    return result_dict

sql1 = """
SELECT a.name, b.order_date 
FROM customers a 
INNER JOIN orders b ON a.id = b.customer_id;"""

sql2 = """
SELECT a.name, b.order_date 
FROM customers a, orders b 
WHERE a.id = b.customer_id;"""

print(is_equi_match_with_llm(sql1, sql2, "http://10.10.202.242:2099/v1", "not_used" ,"qwen2.5-72b-instruct" , 0.7))