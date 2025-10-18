from typing import List, Optional

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
