import glob
import os

from src import _extract


def process_pdfs(path_input: str) -> None:
    """
    Handles file path input, determining if it's a single file or a directory,
    and processes all valid PDF files using _extract.main().

    Args:
        path_input: The folder or single PDF file path provided by the user.
    """
    files_to_process = []

    cleaned_input = path_input.replace("\\", os.sep)

    normalized_path_input = os.path.normpath(cleaned_input)

    # --- 1. Path Existence Check and Resolution ---
    if not os.path.exists(normalized_path_input):
        print(f"Error: The specified path does not exist: '{path_input}'")
        return

    path_obj = os.path.abspath(normalized_path_input)

    # --- 2. Single File Check ---
    if os.path.isfile(path_obj):
        if path_obj.lower().endswith(".pdf"):
            files_to_process.append(path_obj)
        else:
            file_name = os.path.basename(path_obj)
            print(
                f"Error: The file '{file_name}' is not a PDF file. Extension must be '.pdf'."
            )
            return

    # --- 3. Directory Check ---
    elif os.path.isdir(path_obj):
        search_pattern = os.path.join(path_obj, "*.pdf")
        found_files = glob.glob(search_pattern)

        for file_path in found_files:
            if os.path.isfile(file_path):
                files_to_process.append(file_path)

        if not files_to_process:
            print(
                f"Warning: Folder '{path_input}' was scanned, but no PDF files were found."
            )
            return

        print(
            f"Found {len(files_to_process)} PDF file(s) in the directory. Starting processing..."
        )

    # --- 4. Other Path Error Check (e.g., broken symlink) ---
    else:
        print(f"Error: '{path_input}' is an unknown type of file system object.")
        return

    # --- 5. Processing Loop ---
    for file_path in files_to_process:
        file_name = os.path.basename(file_path)
        print(f"\n--- Starting process for: {file_name} ---")
        try:
            _extract.main(file_path)
            print(f"--- Successfully processed: {file_name} ---")

        except Exception as e:
            print(f"--- Failed to process {file_name}. Reason: {e} ---")
            print("--- Moving to the next file (if any). ---")


if __name__ == "__main__":
    file_path_user_input = input("Enter the folder or single pdf file path: ")

    if file_path_user_input.strip():
        process_pdfs(file_path_user_input)
    else:
        print("Input cannot be empty. Exiting.")
