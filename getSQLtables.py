sql = """
WITH age_groups AS (
    SELECT 
        g.gmsfhm, 
        g.xm, 
        strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
        (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
         (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) AS age, 
        CASE
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) <= 18 THEN '0-18'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) BETWEEN 19 AND 24 THEN '19-24'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) BETWEEN 25 AND 34 THEN '25-34'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) BETWEEN 35 AND 44 THEN '35-44'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) BETWEEN 45 AND 54 THEN '45-54'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) BETWEEN 55 AND 64 THEN '55-64'
            WHEN strftime('%Y', 'now') - strftime('%Y', g.csrq) - 
                 (strftime('%m', 'now') < strftime('%m', g.csrq) OR 
                  (strftime('%m', 'now') = strftime('%m', g.csrq) AND strftime('%d', 'now') < strftime('%d', g.csrq))) > 64 THEN '64+'
        END AS age_group
    FROM graph_tag_idcard_base g
),
online_behavior AS (
    SELECT 
        ag.age_group, 
        r.domain, 
        r.app_type, 
        COUNT(*) AS activity_count
    FROM main.rzx_wz_wldt_logs r
    JOIN age_groups ag 
        ON r.auth_account = ag.gmsfhm
    WHERE r.capture_time >= strftime('%s', 'now', '-1 year') * 1000
    GROUP BY ag.age_group, r.domain, r.app_type
),
age_behavior_summary AS (
    SELECT 
        age_group, 
        SUM(activity_count) AS total_activity_count
    FROM online_behavior
    GROUP BY age_group
)
SELECT 
    ob.age_group, 
    ob.domain, 
    ob.activity_count, 
    ROUND(ob.activity_count * 100.0 / ab.total_activity_count, 2) AS activity_percentage,  
    ob.app_type
FROM online_behavior ob
JOIN age_behavior_summary ab
    ON ob.age_group = ab.age_group
ORDER BY ob.age_group, ob.activity_count DESC;
"""

import sqlglot
from sqlglot.expressions import Table

def extract_table_names(sql: str):
    try:
        parsed = sqlglot.parse_one(sql)
        if not parsed:
            return set()

        # 提取 CTE 名称（这些不是实际表）
        cte_names = {cte.alias_or_name for cte in parsed.ctes} if parsed.ctes else set()

        real_tables = set()
        for table in parsed.find_all(Table):
            name = table.name
            if name not in cte_names and not table.args.get("subquery"):
                real_tables.add(name)

        return real_tables

    except Exception as e:
        print(f"[ERROR] SQL 解析失败: {e}")
        return set()

# 提取表名
tables = extract_table_names(sql)
print(tables)
