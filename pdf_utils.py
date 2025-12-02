import io
from PyPDF2 import PdfReader, PdfWriter

def extract_pdf_pages_from_bytes(pdf_bytes: bytes):
    """Extract each page as an independent in-memory PDF buffer."""
    pages = []
    reader = PdfReader(io.BytesIO(pdf_bytes))

    for i in range(len(reader.pages)):
        buf = io.BytesIO()
        writer = PdfWriter()
        writer.add_page(reader.pages[i])
        writer.write(buf)
        buf.seek(0)
        pages.append(buf)

    return pages


def chunk_pages(pages, chunk_size):
    """
    Combine:
        First page + N chunk pages
    into a single PDF per chunk.
    """
    first_page = pages[0]
    rest = pages[1:]

    combined_pdfs = []

    for i in range(0, len(rest), chunk_size):
        buf = io.BytesIO()
        writer = PdfWriter()

        # Add first page
        writer.add_page(PdfReader(first_page).pages[0])

        # Add chunk pages
        for p in rest[i:i+chunk_size]:
            writer.add_page(PdfReader(p).pages[0])

        writer.write(buf)
        buf.seek(0)
        combined_pdfs.append(buf)

    return combined_pdfs
