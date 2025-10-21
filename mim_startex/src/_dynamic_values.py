from typing import Any, Dict, List, Optional

import pandas as pd

from . import _barcodes


def get_value(
    item: pd.DataFrame,
    param: str,
    preferred_columns_list: List[str],
    row: int = 0,
    col: Optional[int] = None,
) -> str:
    try:
        if col is None:
            return item.iloc[row, preferred_columns_list.index(param)].strip()
        return item.iloc[row, col].strip()
    except:
        print(
            f"Error: Couldn't get the value and used '' for \n{param, preferred_columns_list, row} in \n{item}"
        )
        return ""


def extract_one_item(
    data: Dict[str, List[Any]],
    static_values: Dict[str, Any],
    all_barcodes_df: pd.DataFrame,
    item: pd.DataFrame,
    preferred_column_list: List[str],
    preffered_size_list: List[str],
    color_name_index: Optional[int] = None,
) -> None:

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
        data["Total Qty."] = static_values["Total Qty."]

        # Fetch dynamic values
        prepack_code = get_value(item, "Prepack Code", preferred_column_list)
        data["Prepack Code"].append(prepack_code)

        prepacks = get_value(item, "Prepacks", preferred_column_list)
        prepacks = prepacks if prepacks != "" else "0"
        data["Prepacks"].append(prepacks)

        color_name = get_value(
            item,
            "Color Code",
            preferred_column_list,
            row=len(item) - 1,
            col=color_name_index,
        )
        color_code = get_value(item, "Color Code", preferred_column_list)
        color_description = f"{color_code} {color_name}"
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
            barcode_value = _barcodes.get_barcode(
                all_barcodes_df, f"{color_name}", size
            )
        else:
            barcode_value = _barcodes.get_barcode(
                all_barcodes_df, f"{color_code} {color_name}", size
            )
        data["Barcode"].append(barcode_value)
