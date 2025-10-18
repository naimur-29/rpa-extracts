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

    # Specify the column and the repeating value
    COLUMN = 0
    VALUE = "101423"

    # Apply the function
    split_list = split_dataframe_by_value(df, COLUMN, VALUE)

    # list of columns from the po document's table
    # ----- for starting_size = XXS -----
    column_list1 = [
        "Style No",
        "Color Code",
        "XXS",
        "XS",
        "S",
        "M",
        "L",
        "XL",
        "2XL",
        "3XL",
        "ASS",
        "Prepack Code",
        "Prepacks",
        "Qty",
    ]

    size_list1 = [
        "XXS",
        "XS",
        "S",
        "M",
        "L",
        "XL",
        "2XL",
        "3XL",
    ]

    # ----- for starting_size = S -----
    column_list2 = [
        "Style No",
        "Color Code",
        "S",
        "M",
        "L",
        "XL",
        "2XL",
        "",
        "ASS",
        "",
        "",
        "Prepack Code",
        "Prepacks",
        "Qty",
    ]

    size_list2 = [
        "S",
        "M",
        "L",
        "XL",
        "2XL",
    ]

    data = {
        "Prepack Code": [],
        "Prepacks": [],
        "Color Code": [],
        "Size": [],
        "Prepack Qty": [],
        "Qty": [],
        "Style Qty": [],
    }

    def get_value(item, param, preferred_columns_list, row=0):
        # print(item, preferred_columns_list.index(param), "----------------")
        return item.iloc[row, preferred_columns_list.index(param)].strip()

    def extract_one_item(item, preferred_column_list, preffered_size_list):
        for column in preffered_size_list:
            prepack_code = get_value(item, "Prepack Code", preferred_column_list)
            data["Prepack Code"].append(prepack_code)

            prepacks = get_value(item, "Prepacks", preferred_column_list)
            data["Prepacks"].append(prepacks)

            color_name = get_value(item, "Color Code", preferred_column_list, row=1)
            color_code = get_value(item, "Color Code", preferred_column_list)
            color_code += color_name
            data["Color Code"].append(color_code)

            size = column
            data["Size"].append(size)

            prepack_qty = get_value(item, column, preferred_column_list)
            prepack_qty = prepack_qty if prepack_qty != "" else "0"
            data["Prepack Qty"].append(prepack_qty)

            qty = int(prepacks) * int(prepack_qty)
            data["Qty"].append(qty)

            total_qty = get_value(item, "Qty", preferred_column_list)
            total_qty = total_qty.replace(".", "")
            data["Style Qty"].append(total_qty)

    def extract_one_split(split):

        column_list = list(split.iloc[1, :])

        # find the starting size
        ci = 0
        start = column_list[ci]
        while start == "":
            ci += 1
            start = column_list[ci]
        end = "ASS"
        size_list = column_list[column_list.index(start) : column_list.index(end)]

        # Insert missing columns
        color_code_index = column_list.index(start) - 1
        column_list[color_code_index] = "Color Code"
        #   Insert last three column names
        column_list[-3] = "Prepack Code"
        column_list[-2] = "Prepacks"
        column_list[-1] = "Qty"

        for i in range(2, len(split), 2):
            if split.iloc[i, color_code_index] != "":
                item = split[i : i + 2]
            extract_one_item(item, column_list, size_list)

    for split in split_list:
        extract_one_split(split)

    df = pd.DataFrame(data)
    print(df)

else:
    print("No table found in the specified area.")
