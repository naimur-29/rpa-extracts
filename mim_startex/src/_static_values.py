from typing import Any, Dict, List, Optional, Tuple

import pdfplumber


def read_pdf(
    pdf_path: str, page_no_list: Optional[List[int]] = None
) -> Optional[List[str]]:
    """
    Extracts all text from specified page numbers of a PDF file.
    By default (if page_no_list is None), it extracts text from all pages.

    Args:
        pdf_path: A string containing the filepath.
        page_no_list: A list of string containing all the page no. to be extracted. Ex, [1, 2,...]

    Returns:
        A list of strings containing the texts of each page. If nothing's found it returns None.
    """
    try:
        text_by_pages = []
        with pdfplumber.open(pdf_path) as pdf:

            if page_no_list is None:
                pages_to_process = pdf.pages
            else:
                pages_to_process = [pdf.pages[page_no - 1] for page_no in page_no_list]

            for page in pages_to_process:
                text_by_pages.append(page.extract_text())

        return text_by_pages

    except FileNotFoundError:
        print(f"Error: File ({pdf_path}) not found at path: {pdf_path}\n")
        return None
    except IndexError:
        print(
            "Error: Invalid page number specified while extracting PDF file ({pdf_path})!\n"
        )
        return None
    except Exception as e:
        print(f"Error reading PDF file ({pdf_path}): {e}\n")
        return None


def get_value_after_keyphrase(text: str, keyphrase="") -> str | None:
    """
    Given a valid keyphrase tries to return the value after it.
    """
    if keyphrase:
        try:
            value = text.split(keyphrase)[1].strip()
            return value
        except Exception as e:
            print(f"Error: Couldn't find value for ({keyphrase})!", e)
            return None


def get_value_before_keyphrase(text: str, keyphrase="") -> str | None:
    """
    Given a valid keyphrase tries to return the value before it.
    """
    if keyphrase:
        try:
            value = text.split(keyphrase)[0].strip()
            return value
        except Exception as e:
            print(f"Error:", e)
            return None


def get_value_between_keyphrases(
    text: str, keyphrase_left="", keyphrase_right=""
) -> str | None:
    """
    Given a valid keyphrase tries to return the value next to it.
    """
    try:
        value = get_value_after_keyphrase(text, keyphrase_left)
        if value is not None:
            value = get_value_before_keyphrase(value, keyphrase_right)
        else:
            return None
        return value
    except Exception as e:
        print(f"Error:", e)
        return None


def get_static_values(
    file_name: str, target_page_no: int
) -> Tuple[Optional[int], Optional[int], Optional[bool], Dict[str, Any]]:
    print("Fetching static values...")
    print("=========================")
    text_by_page = read_pdf(file_name)

    # Extracted static values dictionary
    static_values = {}

    if text_by_page is not None and text_by_page is not []:
        page1 = text_by_page[target_page_no - 1]
        try:
            page2 = text_by_page[target_page_no]
        except:
            page2 = ""

        # Find out the from what page the barcode starts from
        total_page_no = len(text_by_page)
        barcode_starting_page_no = len(text_by_page)
        for i, page in enumerate(text_by_page):
            if "Colour Code Colour Description Size Barcode" in page:
                barcode_starting_page_no = i + 1
                break

        # Extract Purchase Order
        po_number = get_value_between_keyphrases(page1, "Purchase Order", "\n")
        static_values["Purchase Order"] = po_number

        # Extract Buying House
        buying_house = get_value_before_keyphrase(page1, "Purchase Order")
        buying_house = buying_house.split("\n")[0].strip()
        static_values["Buying House"] = buying_house

        # Extract Buying House Address
        buying_house_address = get_value_before_keyphrase(page1, "Purchase Order")
        buying_house_address = (
            buying_house_address.split(buying_house)[1].replace("\n", " ").strip()
        )

        static_values["Buying House Address"] = buying_house_address

        # Extract Ship-to Address and Total Qty.
        ship_to_address = None
        total_qty = None
        for page in text_by_page:
            if ship_to_address is None:
                ship_to_address = get_value_between_keyphrases(
                    page, "Ship-to Address", "Colour Code"
                )
                ship_to_address = (
                    ship_to_address.replace("\n", ", ")
                    if ship_to_address is not None
                    else ship_to_address
                )
            if total_qty is None:
                total_qty = get_value_after_keyphrase(page, "Total Qty.")
                total_qty = (
                    total_qty.split("\n")[0] if total_qty is not None else total_qty
                )
        static_values["Ship-to Address"] = (
            ship_to_address if ship_to_address is not None else ""
        )
        static_values["Total Qty."] = total_qty if total_qty is not None else "0"

        # Extract Vendor No
        vendor_no = get_value_between_keyphrases(page1, "Vendor No.", "Payment Terms")
        static_values["Vendor No"] = vendor_no

        # Extract Payment Terms
        payment_temrs = get_value_between_keyphrases(
            page1, "Payment Terms", "Prices Including VAT No"
        )
        static_values["Payment Terms"] = payment_temrs

        # Extract Document Date
        document_date = get_value_between_keyphrases(
            page1, "Document Date", "Shipment Method"
        )
        static_values["Document Date"] = document_date

        # Extract Shipment Method
        shipment_method = get_value_between_keyphrases(
            page1, "Shipment Method", "Order Type"
        )
        static_values["Shipment Method"] = shipment_method

        # Extract Order Type
        order_type = get_value_between_keyphrases(page1, "Order Type", "Shipping Agent")
        static_values["Order Type"] = order_type

        # Extract Shipment Agent
        shipping_agent = get_value_between_keyphrases(page1, "Shipping Agent", "\n")
        static_values["Shipping Agent"] = shipping_agent

        # Exract Order for
        order_for = get_value_between_keyphrases(page1, "Order for", "\n")
        static_values["Order for"] = order_for

        # Extract Style No
        style_no = get_value_between_keyphrases(page1, "Qty", " ")
        static_values["Style No"] = style_no

        # Extract Style Description
        style_description = get_value_between_keyphrases(page1, str(style_no), "\n")
        static_values["Style Description"] = style_description

        # Extract HS Code
        hs_code = get_value_between_keyphrases(page1, "HS Code:", "\n")
        static_values["HS Code"] = hs_code

        # Extract Price Including VAT
        price_including_vat = get_value_between_keyphrases(
            page1, "Prices Including VAT", "\n"
        )
        static_values["Price Including VAT"] = price_including_vat

        # Find if 'ASS' is present in the file
        contains_ASS = False
        for page in [page1, page2]:
            context = get_value_between_keyphrases(page, str(style_no), str(hs_code))

            if context is None:
                context = ""

            if contains_ASS is False:
                contains_ASS = " ASS " in context or " ASS\n" in context

        return total_page_no, barcode_starting_page_no, contains_ASS, static_values
    else:
        print("Invalid PDF text!")
    return None, None, None, {}
