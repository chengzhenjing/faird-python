import json
import pyarrow as pa
import pyarrow.compute as pc
import duckdb

def handle_compute_actions(connections, action):
    action_type = action.type
    if action_type == "compute_sum":
        return compute_sum_action(connections, action)
    elif action_type == "compute_mean":
        return compute_mean_action(connections, action)
    elif action_type == "compute_min":
        return compute_min_action(connections, action)
    elif action_type == "compute_max":
        return compute_max_action(connections, action)
    else:
        raise ValueError(f"Unsupported action type: {action_type}")

def compute_sum_action(connections, action):
    arrow_table = get_arrow_table(connections, action)
    column = json.loads(action.body.to_pybytes().decode("utf-8")).get("column")
    sum = pc.sum(arrow_table[column]).as_py()
    return iter([pa.flight.Result(json.dumps({"result": sum}).encode("utf-8"))])

def compute_mean_action(connections, action):
    arrow_table = get_arrow_table(connections, action)
    column = json.loads(action.body.to_pybytes().decode("utf-8")).get("column")
    mean = pc.mean(arrow_table[column]).as_py()
    return iter([pa.flight.Result(json.dumps({"result": mean}).encode("utf-8"))])

def compute_min_action(connections, action):
    arrow_table = get_arrow_table(connections, action)
    column = json.loads(action.body.to_pybytes().decode("utf-8")).get("column")
    mean = pc.min(arrow_table[column]).as_py()
    return iter([pa.flight.Result(json.dumps({"result": mean}).encode("utf-8"))])

def compute_max_action(connections, action):
    arrow_table = get_arrow_table(connections, action)
    column = json.loads(action.body.to_pybytes().decode("utf-8")).get("column")
    mean = pc.max(arrow_table[column]).as_py()
    return iter([pa.flight.Result(json.dumps({"result": mean}).encode("utf-8"))])

def do_limit(arrow_table, row_num):
    return arrow_table.slice(0, row_num)

def do_slice(arrow_table, offset, length):
    return arrow_table.slice(offset, length)

def do_select(arrow_table, columns):
    return arrow_table.select(columns)

def do_filter(arrow_table, expression):
    column_names = arrow_table.column_names
    locals_dict = {col: arrow_table[col].to_pandas() for col in column_names}
    mask = eval(expression, {"__builtins__": None}, locals_dict)
    return arrow_table.filter(pa.array(mask))

def do_sort(arrow_table, column, order):
    if order == "ascending":
        return arrow_table.sort_by(column)
    return arrow_table.sort_by([(column, "descending")])

def do_map(arrow_table, column, func, new_column_name):
    column_data = arrow_table[column].to_pylist()
    mapped_data = [func(value) for value in column_data]
    return arrow_table.append_column(new_column_name, pa.array(mapped_data))

def do_sql(arrow_table, sql_str):
    dataframe = arrow_table
    return duckdb.sql(sql_str).arrow()

def handle_prev_actions(arrow_table, prev_actions):
    for action in prev_actions:
        action_type, params = action
        if action_type == "limit":
            arrow_table = do_limit(arrow_table, params.get("rowNum"))
        elif action_type == "slice":
            arrow_table = do_slice(arrow_table, params.get("offset", 0), params.get("length"))
        elif action_type == "select":
            arrow_table = do_select(arrow_table, params.get("columns"))
        elif action_type == "filter":
            arrow_table = do_filter(arrow_table, params.get("expression"))
        elif action_type == "map":
            arrow_table = do_map(arrow_table, params.get("column"), params.get("func"), params.get("new_column_name", f"{column}_mapped"))
        elif action_type == "sort":
            arrow_table = do_sort(arrow_table, params.get("column"), params.get("order", "ascending"))
        elif action_type == "sql":
            arrow_table = do_sql(arrow_table, params.get("sql_str"))
        else:
            raise ValueError(f"Unsupported action type: {action_type}")
    return arrow_table


def get_arrow_table(connections, action):
    params = json.loads(action.body.to_pybytes().decode("utf-8"))
    dataframe_id = json.loads(params.get("dataframe")).get("id")
    actions = json.loads(params.get("dataframe")).get("actions")
    connection_id = json.loads(params.get("dataframe")).get("connection_id")
    conn = connections[connection_id]
    arrow_table = conn.dataframes[dataframe_id].data
    return handle_prev_actions(arrow_table, actions)