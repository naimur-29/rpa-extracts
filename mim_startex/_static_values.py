import _pdfplumber_func


def get_static_values(file_name, target_page_no):
    text_by_page = _pdfplumber_func.read_pdf(file_name)

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
        po_number = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Purchase Order", "\n"
        )
        static_values["Purchase Order"] = po_number

        # Extract Buying House
        buying_house = _pdfplumber_func.get_value_before_keyphrase(
            page1, "Purchase Order"
        )
        buying_house = buying_house.split("\n")[0].strip()
        static_values["Buying House"] = buying_house

        # Extract Buying House Address
        buying_house_address = _pdfplumber_func.get_value_before_keyphrase(
            page1, "Purchase Order"
        )
        buying_house_address = (
            buying_house_address.split(buying_house)[1].replace("\n", " ").strip()
        )

        static_values["Buying House Address"] = buying_house_address

        # Extract Ship-to Address and Total Qty.
        ship_to_address = None
        total_qty = None
        for page in text_by_page:
            if ship_to_address is None:
                ship_to_address = _pdfplumber_func.get_value_between_keyphrases(
                    page, "Ship-to Address", "Colour Code"
                )
            if total_qty is None:
                total_qty = _pdfplumber_func.get_value_after_keyphrase(
                    page, "Total Qty."
                )
        static_values["Ship-to Address"] = (
            ship_to_address if ship_to_address is not None else ""
        )
        static_values["Total Qty."] = total_qty if total_qty is not None else "0"

        # Extract Vendor No
        vendor_no = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Vendor No.", "Payment Terms"
        )
        static_values["Vendor No"] = vendor_no

        # Extract Payment Terms
        payment_temrs = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Payment Terms", "Prices Including VAT No"
        )
        static_values["Payment Terms"] = payment_temrs

        # Extract Document Date
        document_date = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Document Date", "Shipment Method"
        )
        static_values["Document Date"] = document_date

        # Extract Shipment Method
        shipment_method = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Shipment Method", "Order Type"
        )
        static_values["Shipment Method"] = shipment_method

        # Extract Order Type
        order_type = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Order Type", "Shipping Agent"
        )
        static_values["Order Type"] = order_type

        # Extract Shipment Agent
        shipping_agent = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Shipping Agent", "\n"
        )
        static_values["Shipping Agent"] = shipping_agent

        # Exract Order for
        order_for = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Order for", "\n"
        )
        static_values["Order for"] = order_for

        # Extract Style No
        style_no = _pdfplumber_func.get_value_between_keyphrases(page1, "Qty", " ")
        static_values["Style No"] = style_no

        # Extract Style Description
        style_description = _pdfplumber_func.get_value_between_keyphrases(
            page1, str(style_no), "\n"
        )
        static_values["Style Description"] = style_description

        # Extract HS Code
        hs_code = _pdfplumber_func.get_value_between_keyphrases(page1, "HS Code:", "\n")
        static_values["HS Code"] = hs_code

        # Extract Price Including VAT
        price_including_vat = _pdfplumber_func.get_value_between_keyphrases(
            page1, "Prices Including VAT", "\n"
        )
        static_values["Price Including VAT"] = price_including_vat

        # Find if 'ASS' is present in the file
        contains_ASS = False
        for page in [page1, page2]:
            context = _pdfplumber_func.get_value_between_keyphrases(
                page, str(style_no), str(hs_code)
            )

            if context is None:
                context = ""

            if contains_ASS is False:
                contains_ASS = " ASS " in context or " ASS\n" in context

        return total_page_no, barcode_starting_page_no, contains_ASS, static_values
    else:
        print("Invalid PDF text!")
    return None, None, None, {}
