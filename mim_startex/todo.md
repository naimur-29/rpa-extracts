# Todo - 14th of Sept. 2025

- Reduce dependency (as much as possible).
  - Instead of extracting with two keyphrases, use on keyphrase till you can't.

# Todo - 15th of Sept. 2025

- Find out at what page the barcode starts from ✅
- Figure out a way to fetch the barcode ✅
- Connect the static values with dynamic values ✅
- Handle the 'without prepacks' files

## Exceptions found:

- F PO111094 - 101423 ARJUN - ECOM- NORWAY.pdf
  - Irregular corpped table
- F PO111097 - 101423 ARJUN - NORWAY.pdf
  - Irregular cropped table
- PO111084_1 - 101423 ARJUN.pdf
  - Irregular cropped table

# Todo - 16th of Sept. 2025

- Handle the 'without prepacks' files ✅
  - Identify 'without prepacks' files
- Handle the Irregular table exception ✅
- Handle the 'barcode not in a new page' exception (file: PO111084 - 101423 ARJUN.pdf)
- Reduce dependency (as much as possible).
  - Instead of extracting with two keyphrases, use on keyphrase till you can't.

## Convenient patters found in 'Without Prepacks' files

- No missing values in the tables
- Tables always consists of one row values (helps solve the 'combined columns' irregularity)
- 'Without prepacks' files doesn't contain 'ASS' in size section

### Report Notes - 16th of Sept. 2025

- F PO110969 - 101423 ARJUN - BZ.pdf
  - Multi-line Ship-to addresses ✅
- F PO110944 - 101423 ARJUN - MA.pdf, PO111084 - 101423 ARJUN.pdf, U F PO111092 - 101423 ARJUN - ECOM.pdf
  - Ship-to address missing ✅
- PO111084 - 101423 ARJUN.pdf
  - Barcode table sliced up to the page before ✅
  - Having problem extracting the Total Qty. ✅
  
# Todo - 20th of Sept. 2025
  - Clean code.
  - Show extraction process in a friendly manner.
  - Handle multiple files (folder input) 
  - Generate Output
