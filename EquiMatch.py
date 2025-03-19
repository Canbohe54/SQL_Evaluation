from sqlglot import parse_one
from sqlglot.optimizer import optimize
from sqlglot.expressions import Expression, Alias, Column, Window, CTE, Identifier, Table, TableAlias

def get_alias_map(expression: Expression) -> dict:
    """
    提取 SQL 语法树中的别名映射，包括普通表别名、子查询别名（CTE）、窗口函数别名。
    注意：在建立映射时忽略别名，只匹配结构。
    """
    alias_map = {}
    for node in expression.walk():
        if isinstance(node, Alias):
            alias_map[node.alias] = node.this  # 记录别名映射
        elif isinstance(node, CTE):
            alias_map[node.alias] = node  # 将 CTE 别名加入映射
    return alias_map

def match_aliases(alias_map_sql1: dict, alias_map_sql2: dict) -> dict:
    """
    匹配 SQL1 和 SQL2 之间的等价别名：只关注查询结构，而非别名。
    """
    alias_mapping = {}

    # 获取查询1与查询2之间的严格对应关系，忽略别名，专注结构匹配
    for alias1, node1 in alias_map_sql1.items():
        for alias2, node2 in alias_map_sql2.items():
            if expressions_equal(node1, node2):  # 按照结构匹配
                alias_mapping[alias1] = alias2

    return alias_mapping

def normalize_expression(expression: Expression, alias_mapping: dict) -> Expression:
    """
    替换查询2中的别名，使其结构与查询1一致，确保别名映射正确。
    只替换所有 Identifier 中的 `this` 属性，忽略其他字段。
    `alias_mapping` 使用字符串作为映射。
    """
    expression = expression.copy()

    for node in expression.walk():
        # 只替换 Identifier 节点中的 `this` 属性
        if isinstance(node, Identifier):
            if node.this in alias_mapping:
                # 替换 `this` 属性，直接使用字符串作为 `this`
                node.args["this"] = alias_mapping[node.this]  # 使用字符串替换 `this`

    return expression

def compare_lists_unordered(list1, list2):
    """
    比较两个列表的元素，无视顺序，递归比较每个元素。
    """
    if len(list1) != len(list2):
        return False

    # 对于每个元素进行递归比较
    list2_copy = list2[:]
    for item1 in list1:
        match_found = False
        for item2 in list2_copy:
            if expressions_equal(item1, item2):  # 递归比较每一对元素
                list2_copy.remove(item2)  # 如果找到了匹配项，移除它
                match_found = True
                break
        if not match_found:
            return False  # 如果某个元素没有匹配项，则返回 False

    return True

def expressions_equal(expr1: Expression, expr2: Expression) -> bool:
    """
    递归比较两个 SQL AST 结构是否等价，忽略别名，仅根据结构判断。
    """
    if type(expr1) != type(expr2):
        return False

    def filter_args(expr):
        return {k: v for k, v in expr.args.items() if k not in {"alias", "comments", "parent", "arg_key", "index", "recursive"}}

    args1, args2 = filter_args(expr1), filter_args(expr2)


    if args1.keys() != args2.keys():
        return False

    for key in args1:
        val1, val2 = args1[key], args2[key]

        # 只比较结构，不比较别名
        if isinstance(val1, Expression) and isinstance(val2, Expression):
            if not expressions_equal(val1, val2):
                return False
        elif isinstance(val1, list) and isinstance(val2, list):
            # 在列表比较时，进行无序比较（递归比较每个元素）
            if not compare_lists_unordered(val1, val2):
                # print(expr1.sql(pretty=True), " 与 ", expr2.sql(pretty=True), " 比较失败")
                # print(f"比较失败: {val1} 与 {val2} 不相等")
                return False
        else:
            if val1 != val2:
                return False

    # print(expr1, " 与 ", expr2, " 相等")
    return True

def is_equi_match(sql1: str, sql2: str, dialect: str = "sqlite") -> bool:
    """
    判断两条 SQL 是否等价（基于 AST 解析），自动处理查询1与查询2之间严格的别名映射。
    """
    try:
        expr1 = optimize(parse_one(sql1, dialect=dialect))
        expr2 = optimize(parse_one(sql2, dialect=dialect))

        alias_map_sql1 = get_alias_map(expr1)
        alias_map_sql2 = get_alias_map(expr2)

        alias_mapping = match_aliases(alias_map_sql1, alias_map_sql2)
        # print(f"别名映射: {alias_mapping}")

        expr1 = normalize_expression(expr1, alias_mapping)  # 只修改查询1的别名

        return expressions_equal(expr1, expr2)
    except Exception as e:
        print(f"解析 SQL 出错: {e}")
        return False


# demo
sql_1 = '''
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
    FROM zh_gj_info
    WHERE name = '张三'
) T1919810
WHERE T114514 = 1;
'''

sql_2 = '''
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
    FROM zh_gj_info
    WHERE name = '张三'
)
SELECT 
    id_card, 
    name, 
    to_place_address, 
    to_place, 
    start_time
FROM T1
WHERE T2 = 1;
'''

print(is_equi_match(sql_1, sql_2))
