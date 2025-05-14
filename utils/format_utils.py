from tabulate import tabulate

def format_arrow_table(arrow_table, head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False):
    if display_all:
        return arrow_table.to_pandas().to_string()
    all_columns = arrow_table.column_names
    total_columns = len(all_columns)
    total_rows = arrow_table.num_rows

    if total_columns <= (first_cols + last_cols):
        display_columns = all_columns
    else:
        display_columns = all_columns[:first_cols] + ['...'] + all_columns[-last_cols:]

    head_rows = min(head_rows, total_rows)
    tail_rows = min(tail_rows, total_rows)

    table_data = []
    head_data = arrow_table.slice(0, head_rows)

    for i in range(head_rows):
        row = []
        for col in display_columns:
            if col == '...':
                row.append('...')
            else:
                col_index = all_columns.index(col)
                row.append(str(head_data.column(col_index)[i].as_py()))
        table_data.append(row)

    if total_rows > (head_rows + tail_rows):
        table_data.append(['...' for _ in display_columns])

    if total_rows > head_rows:
        tail_data = arrow_table.slice(total_rows - tail_rows, tail_rows)
        for i in range(tail_rows):
            row = []
            for col in display_columns:
                if col == '...':
                    row.append('...')
                else:
                    col_index = all_columns.index(col)
                    row.append(str(tail_data.column(col_index)[i].as_py()))
            table_data.append(row)

    table_str = tabulate(table_data, headers=display_columns, tablefmt="plain")
    table_str += f"\n\n[{total_rows} rows x {total_columns} columns]"
    return table_str
