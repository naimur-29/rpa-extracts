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
        dropna=False, future_stack=True
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
