import streamlit as st
import pandas as pd
from io import StringIO

from pdf_utils import extract_pdf_pages_from_bytes, chunk_pages
from extract_utils import extract_all_data, extract_from_pdf_chunks_parallel

# Load API key securely
API_KEY = st.secrets["API_KEY"]

st.title("📄 Bank Statement Extractor (Multi-Bank)")

uploaded_file = st.file_uploader("Upload your bank statement PDF")

chunk_size = st.number_input("Chunk size (pages per batch)", min_value=2, max_value=12, value=4)

if uploaded_file is not None:

    pdf_bytes = uploaded_file.read()

    st.info("Extracting first page headers…")

    pages = extract_pdf_pages_from_bytes(pdf_bytes)
    num_pages = len(pages)

    # Extract headers from first page
    first_page_bytes = pages[0].getvalue()

    import requests
    url = "https://extraction-api.nanonets.com/extract"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    fp_file = {"file": ("first.pdf", pages[0], "application/pdf")}
    fp_data = {"output_type": "markdown-financial-docs", "model": "openai"}

    response = requests.post(url, files=fp_file, data=fp_data, headers=headers).json()
    fp_content = response.get("content", "")

    # Extract headers
    import bs4
    soup = bs4.BeautifulSoup(fp_content, "html.parser")
    tables = soup.find_all("table")

    dfs = [pd.read_html(StringIO(str(t)))[0] for t in tables]
    df_headers = max(dfs, key=lambda x: x.shape[1])

    fields = df_headers.columns.tolist()

    st.success(f"Detected {len(fields)} fields from first page.")

    # Chunk pages for async extraction
    if num_pages > 1:
        st.info("Preparing page chunks…")
        chunks = chunk_pages(pages, chunk_size=chunk_size)

        st.info("Extracting data in parallel…")
        pages_data = {
            "output_type": "specified-fields",
            "model": "openai",
            "specified_fields": ", ".join(fields),
        }

        extracted_raw = extract_from_pdf_chunks_parallel(
            chunks, pages_data, API_KEY, max_workers=len(chunks)
        )

        combined_text = "\n---\n".join(extracted_raw)

        final_dfs = []
        for block in combined_text.split("---"):
            df = extract_all_data(block, fields)
            if not df.empty:
                final_dfs.append(df)

        final_df = pd.concat(final_dfs).drop_duplicates().reset_index(drop=True)

        st.success("Extraction Completed!")
        st.dataframe(final_df)

        # Download
        csv = final_df.to_csv(index=False).encode()
        st.download_button("Download as CSV", csv, "extracted.csv")
