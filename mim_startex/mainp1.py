from typing import List, Union

import camelot
import pandas as pd
from camelot import utils

import _barcode
import _static_values

file_name = "./Mim And Manami/US Polo Startex/US Polo-Startex-with Prepack.pdf"
# file_name = (
#     "./Mim And Manami/US Polo Startex/US POLO 2/F PO111097 - 101423 ARJUN - NORWAY.pdf"
# )

# -----------------------------
# Fetch the static values first
total_page_no, barcode_starting_page_no, static_values = (
    _static_values.get_static_values(file_name, target_page_no=1)
)
# -----------------------------

# -----------------------------
# Fetch all barcodes in dataframe for later query
all_barcodes_df = _barcode.get_all_barcodes_df(
    file_name, barcode_starting_page_no, total_page_no
)
# -----------------------------

data = {
    # Static Values
    "Purchase Order": [],
    "Buying House": [],
    "Buying House Address": [],
    "Ship-to Address": [],
    "Vendor No": [],
    "Payment Terms": [],
    "Document Date": [],
    "Shipment Method": [],
    "Order Type": [],
    "Shipping Agent": [],
    "Order for": [],
    "Style No": [],
    "Style Description": [],
    "HS Code": [],
    "Price Including VAT": [],
    # Dynamic Values
    "Prepack Code": [],
    "Prepacks": [],
    "Color Code": [],
    "Size": [],
    "Prepack Qty": [],
    "Qty": [],
    "Style Qty": [],
    "Barcode": [],
}


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


def get_value(item, param, preferred_columns_list, row=0):
    return item.iloc[row, preferred_columns_list.index(param)].strip()


def extract_one_item(item, preferred_column_list, preffered_size_list):
    for column in preffered_size_list:
        # Inject the static values
        data["Purchase Order"] = static_values["Purchase Order"]
        data["Buying House"] = static_values["Buying House"]
        data["Buying House Address"] = static_values["Buying House Address"]
        data["Ship-to Address"] = static_values["Ship-to Address"]
        data["Vendor No"] = static_values["Vendor No"]
        data["Payment Terms"] = static_values["Payment Terms"]
        data["Document Date"] = static_values["Document Date"]
        data["Shipment Method"] = static_values["Shipment Method"]
        data["Order Type"] = static_values["Order Type"]
        data["Shipping Agent"] = static_values["Shipping Agent"]
        data["Order for"] = static_values["Order for"]
        data["Style No"] = static_values["Style No"]
        data["Style Description"] = static_values["Style Description"]
        data["HS Code"] = static_values["HS Code"]
        data["Price Including VAT"] = static_values["Price Including VAT"]

        # Fetch dynamic values
        prepack_code = get_value(item, "Prepack Code", preferred_column_list)
        data["Prepack Code"].append(prepack_code)

        prepacks = get_value(item, "Prepacks", preferred_column_list)
        prepacks = prepacks if prepacks != "" else "0"
        data["Prepacks"].append(prepacks)

        color_name = get_value(
            item, "Color Code", preferred_column_list, row=len(item) - 1
        )
        color_code = get_value(item, "Color Code", preferred_column_list)
        color_description = color_code + color_name
        # Handle Exception for color name 'Greymelange'
        if color_name == "Greymelange":
            color_description = color_name
            color_name = "Grey Mélange"
        data["Color Code"].append(color_description)

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

        # fetch and add barcode
        #   Handle Exception for color name 'Greymelange'
        if color_name == "Grey Mélange":
            barcode_value = _barcode.get_barcode(all_barcodes_df, f"{color_name}", size)
        else:
            barcode_value = _barcode.get_barcode(
                all_barcodes_df, f"{color_code} {color_name}", size
            )
        data["Barcode"].append(barcode_value)


def extract_one_split(split):
    print("-----------------")
    print(split)
    print("-----------------")
    column_list = list(split.iloc[1, :])
    # Clean column names
    column_list = [column_name.strip() for column_name in column_list]
    temp = []
    for column_name in column_list:
        temp.extend(column_name.strip().split("\n"))
    column_list = temp[:]

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
        color_name = split.iloc[i, color_code_index]
        if color_name != "":
            # Handle Exception if color name is 'Greymelange'
            if color_name == "Greymelange":
                item = split[i : i + 1]
                extract_one_item(item, column_list, size_list)

                item = split[i + 1 : i + 2]
                extract_one_item(item, column_list, size_list)
            else:
                item = split[i : i + 2]
                extract_one_item(item, column_list, size_list)


def extract_page(page_number):
    _layout, dim = utils.get_page_layout(file_name)

    # The dimensions are stored in the `dim` variable as a tuple: (width, height)
    width = dim[0]
    height = dim[1]
    # Define the coordinates for the table area
    # Format: 'x1,y1,x2,y2'
    header_offset = 300  # 300 from the second page of barcode, 350 for the first page
    table_coords = [f"0,{height-header_offset},{width}, 0"]

    # Rerun the extraction using the stream flavor and the defined area
    tables = camelot.read_pdf(
        filepath=file_name,
        pages=str(page_number),
        flavor="stream",
        table_areas=table_coords,
        row_tol=0.0,
    )

    if tables.n > 0:
        df = tables[-1].df

        # Specify the column and the repeating value
        COLUMN = 0
        VALUE = static_values["Style No"]

        # Apply the function
        split_list = split_dataframe_by_value(df, COLUMN, VALUE)
        for split in split_list:
            extract_one_split(split)

    else:
        print("No table found in the specified area.")


if total_page_no is not None and barcode_starting_page_no is not None:
    for page_no in range(1, barcode_starting_page_no):
        extract_page(page_no)

df = pd.DataFrame(data)
try:
    df.to_excel("./output.xlsx", index=False)
    print("Saved outupt.xlsx!")
except:
    print("Couldn't save output.xlsx!")
print(df)

