from typing import List, Union

import pandas as pd


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


all_values = {
    "Challan No": [],
    "Challan Date": [],
    "Consignee": [],
    "Delivery Mode": [],
    "Order No": [],
    "Carton": [],
    "Pcs": [],
    "Country": [],
}


def df_find_first_index(df, value_to_find=None):
    # Stack the DataFrame to get a Series with a MultiIndex (row_label, col_label)
    stacked_series = df.stack()

    # Find the index of the first occurrence
    try:
        index_tuple = stacked_series[stacked_series == value_to_find].index[0]
        return index_tuple
    except IndexError:
        return None


def df_find_first_pattern_index(df, pattern=None):
    """
    Finds the (row_label, col_label) of the first cell value
    that matches the given regular expression pattern.

    Args:
        df (pd.DataFrame): The input DataFrame.
        pattern (str): The regular expression pattern to match.

    Returns:
        tuple or None: The (row_label, col_label) tuple of the
                       first match, or None if no match is found.
    """
    if pattern is None:
        return None
    # 1. Stack the DataFrame to get a Series with a MultiIndex (row_label, col_label)
    #    This puts all values into a single column for easy searching.
    stacked_series = df.stack(
        future_stack=True
    )  # Keep NaNs, though str methods handle them

    # 2. Use str.contains() with the regex pattern
    #    We must handle non-string (e.g., numeric) values by converting them to string
    #    or handling the error if the value is not a string. 'na=False' treats NaNs as non-matches.
    mask = stacked_series.astype(str).str.contains(pattern, na=False)

    # 3. Find the index of the first occurrence where the mask is True
    try:
        # Get the MultiIndex where the mask is True and take the first one
        index_tuple = stacked_series[mask].index[0]
        return index_tuple
    except IndexError:
        # This occurs if the Series subset is empty (no match found)
        return None


def extract_static_values(split):
    split_columns = list(split.columns)
    static_values = {
        "Challan No": "",
        "Challan Date": "",
        "Consignee": "",
        "Delivery Mode": "",
    }

    # extract Challan No
    challan_no_index = df_find_first_index(split, "Challan No:")
    challan_no_col_index = split_columns.index(challan_no_index[1])

    challan_no = ""
    i = 1
    while (challan_no == "" or challan_no == "nan") and challan_no_col_index + i < len(
        split_columns
    ):
        challan_no = str(split.iloc[challan_no_index[0], challan_no_col_index + i])
        i += 1
    static_values["Challan No"] = challan_no.replace("O", "0")

    # extract Challan Date
    challan_date_index = df_find_first_index(split, "Date")
    challan_date_col_index = split_columns.index(challan_date_index[1])

    challan_date = ""
    i = 1
    while len(challan_date) != 19 and challan_date_col_index + i < len(split_columns):
        challan_date = str(
            split.iloc[challan_date_index[0], challan_date_col_index + i]
        )
        i += 1
    static_values["Challan Date"] = challan_date.split(" ")[0]

    # extract consignee
    consignee_index = df_find_first_index(split, "To:")
    consignee_col_index = split_columns.index(consignee_index[1])

    consignee = ""
    i = 0
    while (consignee == "" or consignee == "nan") and consignee_col_index + i < len(
        split_columns
    ):
        consignee = str(split.iloc[consignee_index[0] + 1, consignee_col_index + i])
        i += 1
    static_values["Consignee"] = consignee.split("\n")[0]

    # extract Delivery Mode
    delivery_mode_index = df_find_first_index(split, "Delivery Mode")
    delivery_mode_col_index = split_columns.index(delivery_mode_index[1])

    delivery_mode = ""
    i = 1
    while (
        delivery_mode == "" or delivery_mode == "nan" or delivery_mode == ":"
    ) and delivery_mode_col_index + i < len(split_columns):
        delivery_mode = str(
            split.iloc[delivery_mode_index[0], delivery_mode_col_index + i]
        )
        i += 1
    static_values["Delivery Mode"] = delivery_mode

    return static_values


def extract_one_split(split, all_values):
    split = split.reset_index(drop=True)

    static_values = extract_static_values(split)
    print(static_values)

    # Find Order No starting index
    order_no_pattern = r"^\d+-\d+$"
    order_no_starting_index = df_find_first_pattern_index(
        df=split, pattern=order_no_pattern
    )
    print("👾👾👾👾👾👾👾👾👾👾👾")
    print(order_no_starting_index)
    print("👾👾👾👾👾👾👾👾👾👾👾")


excel_file = pd.ExcelFile("./data/H&M Challan 1 ST CUT OFF WEEK-05.xlsx")
sheet_names = excel_file.sheet_names
print("Found the following sheets:")
print(sheet_names)

print("Parsing", sheet_names[0])
df = pd.read_excel(
    "./data/H&M Challan 1 ST CUT OFF WEEK-05.xlsx", sheet_name=sheet_names[0]
)
print(df)
df.to_excel("./data/out.xlsx")

dfs = split_dataframe_by_value_and_truncate(
    df=df,
    split_column_name="Unnamed: 3",
    split_value="Impress-Newtex\nComposite Textiles Ltd.",
    end_marker_value="Total :",
)
dfs[0].iloc[:, :13].to_excel("./data/split1.xlsx")
dfs[0].iloc[:, :13].to_excel("./data/split2.xlsx")

splits = [df.iloc[:, :15] for df in dfs]

for split in splits:
    extract_one_split(split=split, all_values=all_values)
