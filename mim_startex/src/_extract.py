from typing import Any, Dict, List, Optional, Union

import camelot
import pandas as pd
from camelot import utils
from tqdm import tqdm

from . import _barcodes, _dynamic_values, _static_values


def split_dataframe_by_value_and_truncate(
    df: pd.DataFrame,
    split_column_name: Union[str, int],
    split_value: str,
    end_marker_value: str,
) -> List[pd.DataFrame]:
    """
    Splits a DataFrame into smaller DataFrames, where each split starts
    with a row containing the specified 'split_value' in 'split_column_name'.

    Each resulting split is then truncated to end *before* the first row
    that contains the 'end_marker_value' in *any* column. Rows before the
    very first instance of the split_value are also removed.
    """

    # 1. Find the index of the first occurrence of the split_value
    first_split_index = df[split_column_name].eq(split_value).idxmax()

    # 2. Slice the DataFrame to start from this first split point
    df_sliced = df.loc[first_split_index:].copy()

    # 3. Create a boolean Series for the splitting points (on the sliced DataFrame)
    is_split_point = df_sliced[split_column_name] == split_value

    # 4. Use cumsum() to create a unique group ID for each segment
    group_ids = is_split_point.cumsum()

    # 5. Group and Truncate the parts
    split_dfs = []

    for _group_id, group_df in df_sliced.groupby(group_ids):
        is_end_marker = (group_df == end_marker_value).any(axis=1)

        if is_end_marker.any():
            first_marker_row_index = is_end_marker.idxmax()

            marker_position = group_df.index.get_loc(first_marker_row_index)

            truncated_df = group_df.iloc[:marker_position]

            if not truncated_df.empty:
                split_dfs.append(truncated_df)
        else:
            split_dfs.append(group_df)

    return split_dfs


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
        print("Error: Delimiter character (char) cannot be None.")
        return df

    new_data = {}
    col_offset = 0

    for col in df.columns:
        current_series = df[col]

        if current_series.astype(str).str.contains(char, regex=False).any():

            # 1. Split the column data. `expand=True` creates a temporary DataFrame
            split_data = current_series.str.split(char, expand=True)
            split_data = split_data.fillna("")

            # 2. Add the first part (the 'head') to the new DataFrame structure
            new_col_key = col + col_offset
            new_data[new_col_key] = split_data[0]

            # 3. Add the remaining split parts as new columns (injected beside)
            for i in range(1, split_data.shape[1]):
                col_offset += 1  # Increment offset for the new injected column
                injected_col_key = col + col_offset
                new_data[injected_col_key] = split_data[i]

        else:
            new_col_key = col + col_offset
            new_data[new_col_key] = current_series.fillna("")

    new_df = pd.DataFrame(new_data)
    new_df.columns = range(new_df.shape[1])

    return new_df


def extract_one_split(
    split: pd.DataFrame,
    is_without_prepacks: bool,
    data: Dict[str, List[Any]],
    static_values: Dict[str, Any],
    all_barcodes_df: pd.DataFrame,
) -> None:
    # Handle 'combined columns irregularity' for 'without prepacks' files
    split = split_combined_columns_df(split, "\n")

    column_list = list(split.iloc[1, :])
    # Clean column names
    column_list = [column_name.strip() for column_name in column_list]
    temp = []
    for column_name in column_list:
        temp.extend(column_name.strip().split("\n"))
    column_list = temp[:]

    # Truncate the `size_list` from the `column_list`
    #   Find the starting and ending index for size list
    start = 0
    while column_list[start] == "":
        start += 1

    end = start
    while column_list[end] != "":
        end += 1

    # For with prepacks -------------------
    size_list = column_list[start:end]
    if not is_without_prepacks:
        size_list = column_list[start : column_list.index("ASS")]

    # Insert missing columns
    color_code1_index = 0
    #   Find the color code index
    while split.iloc[2, color_code1_index] == "":
        color_code1_index += 1
    column_list[color_code1_index] = "Color Code"

    # Find the color name index
    if len(split) <= 3:
        color_code2_index = color_code1_index
    else:
        color_code2_index = 0
        while split.iloc[3, color_code2_index] == "":
            color_code2_index += 1

    #   Insert last n column names
    column_list[-1] = "Qty"

    # For with prepacks -------------------
    if not is_without_prepacks:
        column_list[-2] = "Prepacks"
        column_list[-3] = "Prepack Code"

    for i in range(2, len(split), 2):
        color_name1 = split.iloc[i, color_code1_index]

        if color_name1 != "":
            # Handle Exception if color name is 'Greymelange'
            if color_name1 == "Greymelange":
                item = split[i : i + 1]
                _dynamic_values.extract_one_item(
                    data, static_values, all_barcodes_df, item, column_list, size_list
                )

                if i + 1 < len(split):
                    item = split[i + 1 : i + 2]
                    _dynamic_values.extract_one_item(
                        data,
                        static_values,
                        all_barcodes_df,
                        item,
                        column_list,
                        size_list,
                        color_code2_index,
                    )
            else:
                item = split[i : i + 2]
                _dynamic_values.extract_one_item(
                    data,
                    static_values,
                    all_barcodes_df,
                    item,
                    column_list,
                    size_list,
                    color_code2_index,
                )


def extract_page(
    page_number: int,
    file_name: str,
    split_to_join: Optional[pd.DataFrame],
    static_values: Dict[str, Any],
    all_barcodes_df: pd.DataFrame,
    is_without_prepacks: bool,
    data: Dict[str, List[Any]],
) -> None:
    _layout, dim = utils.get_page_layout(file_name)

    width = dim[0]
    height = dim[1]
    header_offset = 350 if split_to_join is None else 300
    table_coords = [f"0,{height-header_offset},{width}, 0"]

    tables = camelot.read_pdf(
        filepath=file_name,
        pages=str(page_number),
        flavor="stream",
        table_areas=table_coords,
    )

    if tables.n > 0:
        df = tables[-1].df

        if split_to_join is not None:
            df_cols = set(df.columns)
            split_to_join_cols = set(split_to_join.columns)
            columns_to_drop = list(split_to_join_cols - df_cols)

            split_to_join_cleaned = split_to_join.drop(columns=columns_to_drop)

            df = pd.concat([split_to_join_cleaned, df], ignore_index=True)

        split_column_no = 0
        split_value = str(static_values["Style No"])
        end_marker_value = f"HS Code: {str(static_values["HS Code"])}"

        split_list = split_dataframe_by_value_and_truncate(
            df, split_column_no, split_value, end_marker_value
        )
        for split in split_list:
            if split[0].isin([static_values["Style No"]]).any():
                if len(split) > 1:
                    extract_one_split(
                        split, is_without_prepacks, data, static_values, all_barcodes_df
                    )
                else:
                    split_to_join = split

    else:
        print("No table found in the specified area.")


def main(file_name: str) -> None:
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
        "Total Qty.": [],
    }

    # -----------------------------
    # Fetch the static values first
    total_page_no, barcode_starting_page_no, contains_ASS, static_values = (
        _static_values.get_static_values(file_name, target_page_no=1)
    )
    is_without_prepacks = not contains_ASS

    print("-------------------")
    if is_without_prepacks:
        print("Without Prepacks")
        print(static_values)
    else:
        print("With Prepacks")
    print("-------------------")
    # -----------------------------

    # -----------------------------
    # Fetch all barcodes in dataframe for later query
    all_barcodes_df = _barcodes.get_all_barcodes_df(
        file_name, barcode_starting_page_no, total_page_no
    )
    # -----------------------------

    # If split found, then join it with the next one
    split_to_join = None

    if total_page_no is not None and barcode_starting_page_no is not None:
        print("Extracting dynamic values:")
        print("==========================")
        for page_no in tqdm(range(1, barcode_starting_page_no)):
            extract_page(
                page_no,
                file_name,
                split_to_join,
                static_values,
                all_barcodes_df,
                is_without_prepacks,
                data,
            )

    print("-------------------")
    print("Extraction Complete!")
    print("-------------------")
    df = pd.DataFrame(data)
    try:
        out_file_name = (
            f"{file_name.split("/")[-1].split("\\")[-1].split(".pdf")[0]}.xlsx"
        )
        df.to_excel(out_file_name, index=False)
        print(f"Saved as {out_file_name} in the base directory!")
    except:
        print("Couldn't save output.xlsx!")
