import camelot
import pandas as pd
from camelot import utils


def extract_barcodes_from_page(
    file_name, table_coords, page_number, barcode_starting_page_no
):
    tables = camelot.read_pdf(
        filepath=file_name,
        pages=str(page_number),
        flavor="stream",
        table_areas=table_coords,
    )

    if tables.n > 0:
        df = tables[-1].df
        # print(df)

        found_barcode = False
        k = 0
        while not found_barcode and k <= len(df):
            try:
                if not page_number == barcode_starting_page_no:
                    int(df.iloc[k, -1])
                    found_barcode = True
                else:
                    found_barcode = df.iloc[k - 1, -1] == "Barcode"
            except:
                pass
            k += 1

        barcodes_df = df.iloc[k - 1 :, :]

        if page_number == barcode_starting_page_no:
            new_barcodes_df = pd.DataFrame({})
            col_count = 0
            for col in df.columns:
                if barcodes_df[col].iloc[0] != "" or barcodes_df[col].iloc[1] != "":
                    new_barcodes_df[col_count] = barcodes_df[col]
                    col_count += 1
            barcodes_df = new_barcodes_df

        return barcodes_df
    else:
        print("No table found in the specified area.")
    return pd.DataFrame({})


def get_all_barcodes_df(file_name, barcode_starting_page_no, total_pages):
    _layout, dim = utils.get_page_layout(file_name)

    # The dimensions are stored in the `dim` variable as a tuple: (width, height)
    width = dim[0]
    height = dim[1]
    # Define the coordinates for the table area
    # Format: 'x1,y1,x2,y2'
    header_offset = 300  # 300 from the second page of barcode, 350 for the first page
    table_coords = [f"0,{height-header_offset},{width}, 0"]

    all_barcodes_df = pd.DataFrame({})
    for page_number in range(barcode_starting_page_no, total_pages + 1):
        barcodes_df = extract_barcodes_from_page(
            file_name, table_coords, page_number, barcode_starting_page_no
        )
        all_barcodes_df = pd.concat([all_barcodes_df, barcodes_df], ignore_index=True)

    return all_barcodes_df


def get_barcode(df, color_code, size):
    try:
        # 1. Define your conditions
        condition_A = df.iloc[:, -3] == color_code
        condition_B = df.iloc[:, -2] == size

        # 2. Use .loc to filter based on both conditions and select 'col c'
        res = df.loc[condition_A & condition_B, :]
        res = res.iloc[0, -1]
        return res
    except:
        print(f"Couldn't fetch barcode for color code ({color_code}), size ({size})!)")
