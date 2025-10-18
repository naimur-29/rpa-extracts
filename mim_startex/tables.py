import camelot
from camelot import utils

file_name = "./Mim And Manami/US Polo Startex/US Polo-Startex-with Prepack.pdf"
page_number = "1"  # Specify the page you're interested in

# Get the page layout and dimensions
# This function is not part of the public API, but it's a known workaround.
layout, dim = utils.get_page_layout(file_name)

# The dimensions are stored in the `dim` variable as a tuple: (width, height)
width = dim[0]
height = dim[1]
# Define the coordinates for the table area
# Format: 'x1,y1,x2,y2'                       ---------------------------------------
header_offset = 300  # 300 from the second page of barcode, 350 for the first page
table_coords = [f"0,{height-header_offset},{width}, 0"]
#                                              ---------------------------------------

# Rerun the extraction using the stream flavor and the defined area
tables = camelot.read_pdf(
    filepath=file_name, pages=page_number, flavor="stream", table_areas=table_coords
)

from typing import List, Union

import pandas as pd


def split_dataframe_by_value(
    df: pd.DataFrame, column_name: Union[str, int], split_value: any
) -> List[pd.DataFrame]:
    """
    Splits a DataFrame into smaller DataFrames, where each split starts
    with a row containing the specified 'split_value' in 'column_name'.

    This version also removes all rows that occur before the very first
    instance of the split_value.
    """

    # 1. Find the index of the first occurrence of the split_value
    first_split_index = df[column_name].eq(split_value).idxmax()

    # 2. Slice the DataFrame to start from this first split point
    # This removes any leading rows before the first split_value
    df_sliced = df.loc[first_split_index:].copy()

    # 3. Create a boolean Series for the splitting points (on the sliced DataFrame)
    is_split_point = df_sliced[column_name] == split_value

    # 4. Use cumsum() to create a unique group ID for each segment
    # This Series increments only when the split_value is encountered.
    group_ids = is_split_point.cumsum()

    # 5. Use groupby() to split the DataFrame and collect the parts
    split_dfs = []

    # The groupby object yields (group_id, group_dataframe) tuples
    # Since we sliced the df to start at the first split point, group_id will
    # start at 1, and every split will begin with the split_value.
    for group_id, group_df in df_sliced.groupby(group_ids):
        split_dfs.append(group_df)

    return split_dfs


# You can now access your table as a DataFrame
if tables.n > 0:
    df = tables[-1].df
    # print(df.head())
    print(df)
    print(df.columns, "------------")

    # Specify the column and the repeating value
    COLUMN = 0
    VALUE = "101423"

    # Apply the function
    split_list = split_dataframe_by_value(df, COLUMN, VALUE)
    item = split_list[0][2:4]
    print(item)
    print(item.iloc[0, 1])

    # if split_list[0][2:3].iloc[0, 1]:
    #     print("works")

    size_list = ["XXS", "XS", "S", "M", "L", "XL", "2XL", "3XL"]
    param_index_list = {
        "Style No": 0,
        "Color Code": 1,
        "XXS": 2,
        "XS": 3,
        "S": 4,
        "M": 5,
        "L": 6,
        "XL": 7,
        "2XL": 8,
        "3XL": 9,
        "ASS": 10,
        "Prepack Code": 11,
        "Prepacks": 12,
        "Qty": 13,
    }

    data = {
        "Prepack Code": [],
        "Prepacks": [],
        "Color Code": [],
        "Size": [],
        "Prepack Qty": [],
        "Qty": [],
        "Style Qty": [],
    }

    def get_value(item, param, row=0):
        return item.iloc[row, param_index_list[param]].strip()

    for size in size_list:
        prepack_code = get_value(item, "Prepack Code")
        data["Prepack Code"].append(prepack_code)

        prepacks = get_value(item, "Prepacks")
        data["Prepacks"].append(prepacks)

        color_name = get_value(item, "Color Code", row=1)
        color_code = get_value(item, "Color Code")
        color_code += color_name
        data["Color Code"].append(color_code)

        data["Size"].append(size)

        prepack_qty = get_value(item, size)
        prepack_qty = prepack_qty if prepack_qty != "" else "0"
        data["Prepack Qty"].append(prepack_qty)

        qty = int(prepacks) * int(prepack_qty)
        data["Qty"].append(qty)

        total_qty = get_value(item, "Qty")
        total_qty = total_qty.replace(".", "")
        data["Style Qty"].append(total_qty)

    print(data)
    df = pd.DataFrame(data)
    print(df)

    # Print the results
    # for i, sub_df in enumerate(split_list):
    #     print(f"\n--- Split {i+1} (Group ID {i+1}) ---")
    #     print(sub_df)
else:
    print("No table found in the specified area.")
