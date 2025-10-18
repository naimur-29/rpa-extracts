from typing import List, Union

import camelot
import pandas as pd
from camelot import utils

import _barcode
import _static_values

file_name = "./Mim And Manami/US Polo Startex/US Polo-Startex-without Prepack.pdf"
file_name = "./Mim And Manami/US Polo Startex/US Polo-Startex-with Prepack.pdf"
# -----------------------------
# Fetch the static values first
total_page_no, barcode_starting_page_no, contains_ASS, static_values = (
    _static_values.get_static_values(file_name, target_page_no=1)
)
is_without_prepacks = not contains_ASS

print("-------------------")
if is_without_prepacks:
    print("Without Prepacks")
else:
    print("With Prepacks")
print("-------------------")
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


def split_combined_columns_df(df: pd.DataFrame, char: str) -> pd.DataFrame:
    """
    Splits cells containing a specified character (e.g., newline '\n') across multiple
    new columns, fixing column misalignment typical in table extraction.

    Iterates through the DataFrame columns. If any cell in a column contains the
    `char` delimiter, the cell's content is split, and the resulting parts are
    injected as new columns immediately to the right, shifting subsequent columns.

    Args:
        df (pd.DataFrame): The input DataFrame, typically from table extraction.
        char (str): The character used to delimit combined values (e.g., '\\n').
                    Defaults to '\\n'.

    Returns:
        pd.DataFrame: A new DataFrame with split columns and aligned data.
                      NaN values resulting from the split are converted to "".
    """
    if not char:
        # Note: The default argument now handles this, but keeping a check is safe.
        print("Error: Delimiter character (char) cannot be None.")
        return df

    new_data = {}
    col_offset = 0

    # Iterate through the columns of the original DataFrame
    for col in df.columns:
        # Get the column data as a Series
        current_series = df[col]

        # Check if ANY element in the column contains the delimiter character
        # We use .astype(str) to safely check for the substring, handling NaNs
        if current_series.astype(str).str.contains(char, regex=False).any():

            # 1. Split the column data. `expand=True` creates a temporary DataFrame
            #    where each part of the split is in a new column (0, 1, 2, ...).
            split_data = current_series.str.split(char, expand=True)

            # --- Feature: Replace NaN/None in split parts with "" ---
            # Fill NaN values in the split data with empty strings. This ensures
            # that where a split part doesn't exist (e.g., only 3 values were split
            # but the max was 5), the cell is represented by "" instead of NaN.
            split_data = split_data.fillna("")

            # 2. Add the first part (the 'head') to the new DataFrame structure
            #    This part replaces the original column's position (col + current offset).
            new_col_key = col + col_offset
            new_data[new_col_key] = split_data[0]

            # 3. Add the remaining split parts as new columns (injected beside)
            #    Iterate from the second part (index 1) up to the max split.
            for i in range(1, split_data.shape[1]):
                col_offset += 1  # Increment offset for the new injected column
                injected_col_key = col + col_offset
                new_data[injected_col_key] = split_data[i]

        else:
            # If no split is needed, just add the original column's data.
            # Convert any existing None/NaN in the original series to "" for consistency
            # if we expect string data, otherwise we rely on the final DataFrame construction.
            # Keeping the original values (including NaN) here is usually safer.
            new_col_key = col + col_offset
            new_data[new_col_key] = current_series.fillna("")

    # Create the final DataFrame from the new_data dictionary
    # The keys (col + col_offset) are already unique and in the correct order.
    new_df = pd.DataFrame(new_data)

    # Final touch: Reset column names to a clean integer sequence (0, 1, 2, ...)
    new_df.columns = range(new_df.shape[1])

    return new_df


def extract_one_split(split):
    print("-----------------")
    print(split)
    print("-----------------")
    # Handle 'combined columns irregularity' for 'without prepacks' files
    split = split_combined_columns_df(split, "\n")
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

    # truncate the size list
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
        color_name1 = split.iloc[i, color_code_index]
        if color_name1 != "":
            # Handle Exception if color name is 'Greymelange'
            if color_name1 == "Greymelange":
                item = split[i : i + 1]
                extract_one_item(item, column_list, size_list)

                color_name2 = split.iloc[i + 1, color_code_index]
                if color_name2 == "Greymelange":
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
    header_offset = 350  # 300 from the second page of barcode, 350 for the first page
    # 350 works for solving the 'combined column' problem
    table_coords = [f"0,{height-header_offset},{width}, 0"]

    # Rerun the extraction using the stream flavor and the defined area
    tables = camelot.read_pdf(
        filepath=file_name,
        pages=str(page_number),
        flavor="stream",
        table_areas=table_coords,
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
