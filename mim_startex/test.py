import re

import pandas as pd


def extract_po_data(text: str):
    """
    Extracts structured purchase order line item data from raw text.

    This function uses regex and state tracking to parse the irregular format,
    where Style No. and size headers are separate from the detail lines, and
    color information follows the data line.

    Args:
        text: The raw purchase order text.

    Returns:
        A pandas DataFrame containing the extracted and structured line item data.
    """
    lines = text.strip().split("\n")

    # State variables to track the current context across lines
    current_style_no = ""
    current_description = ""
    current_sizes = []

    extracted_data = []

    # Regex Patterns
    # 1. Matches a new style line, e.g., '101423 USPA T-Shirt Arjun Men'
    style_pattern = re.compile(r"^(\d{6})\s+(.*)\s*$", re.IGNORECASE)
    # 2. Matches a simple color line, e.g., 'White' or 'Pale Banana'
    # Only captures single/double capitalized words (e.g., 'Pale Banana')
    color_pattern = re.compile(r"^\s*([A-Z][a-z\s]+)\s*$")
    # 3. Matches a line with Prepack Code + data (the detail line)
    # Captures Prepack Code (XX-XXXXTCX) and all subsequent numeric tokens.
    detail_pattern = re.compile(r"^([0-9]{2}-[0-9]{4}[A-Z]{3})\s+([\d\s\.]+)$")

    def clean_qty(qty_raw: str) -> int:
        """Heuristic to handle '1.020' as 1020, while keeping others as standard integers."""
        # 1.020 is likely 1,020 pieces, as other quantities are large whole numbers.
        if (
            qty_raw.count(".") == 1
            and len(qty_raw.split(".")[-1]) == 3
            and qty_raw.split(".")[0] in ["1", "0"]
        ):
            return int(float(qty_raw) * 1000)
        try:
            # Fallback to standard integer conversion
            return int(float(qty_raw.replace(",", "")))
        except ValueError:
            return 0

    for line in lines:
        line = line.strip()
        # Skip empty lines, separators, and non-data headers
        if not line or line.startswith("HS Code") or line.startswith("Style No."):
            continue

        # --- 1. Check for new Style/Description ---
        match_style = style_pattern.match(line)
        if match_style:
            # New style header, reset context for the new block
            current_style_no = match_style.group(1)
            current_description = match_style.group(2).strip()
            current_sizes = []  # Sizes will follow immediately
            continue

        # --- 2. Check for Size Headers ---
        # A line of sizes usually follows the style description if current_sizes is empty.
        # This is a heuristic: check if all parts are short codes (<= 4 chars) and there are at least 3 parts.
        if current_style_no and not current_sizes:
            parts = line.split()
            if len(parts) >= 3 and all(
                len(p) <= 4 and p.replace("-", "").isalnum() for p in parts
            ):
                current_sizes = parts
                continue

        # --- 3. Check for Detail Line (Prepack Code + data) ---
        match_detail_line = detail_pattern.match(line)
        if match_detail_line and current_style_no:
            prepack_code = match_detail_line.group(1)
            data_string = match_detail_line.group(2).strip()

            # Extract all numeric-like tokens (including '01', '1.020')
            data_parts = re.findall(r"[\d\.]+", data_string)

            if len(data_parts) >= 2:
                try:
                    # The last two parts are consistently Prepacks and Qty
                    qty = clean_qty(data_parts[-1])
                    prepacks = int(data_parts[-2])

                    # The rest forms the size breakdown values
                    size_breakdown_values = data_parts[:-2]

                    # Map available values to size headers
                    breakdown_map = dict(zip(current_sizes, size_breakdown_values))

                    # Create a clean size breakdown string for the table
                    breakdown_str = " ".join(
                        [f"{k}:{breakdown_map.get(k, '-')}" for k in current_sizes]
                    )

                    extracted_data.append(
                        {
                            "Style No.": current_style_no,
                            "Description": current_description,
                            "Prepack Code": prepack_code,
                            "Prepacks": prepacks,
                            "Qty": qty,
                            "Color": "N/A",  # Placeholder, updated by next line
                            "Size Breakdown": breakdown_str,
                            "Size Headers": " ".join(current_sizes),
                        }
                    )
                    continue

                except ValueError:
                    # Ignore lines that look like data but fail conversion
                    pass

        # --- 4. Check for Color Line ---
        # Color line follows the data line in the PO. We retroactively apply it to the last record.
        match_color = color_pattern.match(line)
        if match_color and extracted_data:
            color = match_color.group(1).strip()
            # Find the most recent record that is part of the current style and is missing a color
            for record in reversed(extracted_data):
                if record["Style No."] == current_style_no and record["Color"] == "N/A":
                    record["Color"] = color
                    break
            continue

    df = pd.DataFrame(extracted_data)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "Style No.",
                "Description",
                "Color",
                "Prepack Code",
                "Size Headers",
                "Size Breakdown",
                "Prepacks",
                "Qty",
            ]
        )

    # Select and reorder final columns for presentation
    final_columns = [
        "Style No.",
        "Description",
        "Color",
        "Prepack Code",
        "Size Headers",
        "Size Breakdown",
        "Prepacks",
        "Qty",
    ]

    return df[final_columns]


# The input text provided
po_text = """
SGH Fashion
Road no 12/B, House no-1237, 4th. Floor
Avenue -10, Mirpur DOHS, Dhaka-1216
Dhaka, 1216
Bangladesh
Purchase Order PO110314
Page 1
Vendor No. 1713082102 Payment Terms TT50 days (B/L) Prices Including VAT No
Document Date 01-08-24 Shipment Method Order Type Collection Sale
Shipping Agent SCANGLOBAL
Order for Norway
Style No. Description Prepack Code Prepacks Qty
101423 USPA T-Shirt Arjun Men
XXS XS S M L XL 2XL 3XL ASS
11-0601TCX 1 3 4 3 1 01 85 1.020
White
11-0601TCX 12 69 2 24
White
11-0601TCX 12 70 2 24
White
11-0601TCX 12 71 1 12
White
11-0601TCX 6 D3 1 6
White
11-0601TCX 2 2 2 L5 8 48
White
11-0601TCX 1 3 2 L6 14 84
White
HS Code: 6109100010
101423 USPA T-Shirt Arjun Men
S M L XL 2XL ASS
12-0824TCX 1 3 4 3 1 01 8 96
Pale Banana
HS Code: 6109100010
101423 USPA T-Shirt Arjun Men
S M L XL 2XL ASS
14-2311TCX 1 3 4 3 1 01 22 264
Prism Pink
HS Code: 6109100010
101423 USPA T-Shirt Arjun Men
S M L XL 2XL ASS
15-1245TCX 1 3 4 3 1 01 33 396
Mock Orange
HS Code: 6109100010
101423 USPA T-Shirt Arjun Men
S M L XL 2XL ASS
15-3920TCX 1 3 4 3 1 01 19 228
Placid Blu
"""

# Execute extraction and print the result in a readable Markdown format
result_df = extract_po_data(po_text)
print("## Extracted Purchase Order Data")
print(result_df)
