import streamlit as st
import pdfplumber
from io import StringIO
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
from PyPDF2 import PdfReader, PdfWriter
import io

# ------------------------------------------------------
# STREAMLIT UI
# ------------------------------------------------------
st.title("Bank Statement Extractor (Nanonets)")
st.write("Upload bank statement PDF file to extract transaction tables.")

uploaded_file = st.file_uploader("Upload Bank PDF Statement", type=["pdf"])

if uploaded_file is not None:

    st.info("Processing PDF... Please wait.")

    try:
        start = time.time()

        # ------------------------------------------------------
        # LOAD PDF IN MEMORY
        # ------------------------------------------------------
        reader = PdfReader(uploaded_file)

        # --------------------------------------
        # Extract FIRST PAGE (in memory)
        # --------------------------------------
        first_page_buffer = io.BytesIO()
        first_page_writer = PdfWriter()
        first_page_writer.add_page(reader.pages[0])
        first_page_writer.write(first_page_buffer)
        first_page_buffer.seek(0)

        files_first_page = {
            "file": ("first_page_only.pdf", first_page_buffer, "application/pdf")
        }

        # ------------------------------------------------------
        # SEND FIRST PAGE → FIND TABLE STRUCTURE
        # ------------------------------------------------------
        API_KEY = st.secrets["API_KEY"]
        url = "https://extraction-api.nanonets.com/extract"
        headers = {"Authorization": f"Bearer {API_KEY}"}

        data1 = {
            "output_type": "markdown",
            "model": "nanonets"
        }

        response_first = requests.post(url, headers=headers, files=files_first_page, data=data1)
        markdown_first = response_first.json()["content"]

        soup_first = BeautifulSoup(markdown_first, "html.parser")
        tables_first = soup_first.find_all("table")

        dfs_first = []
        for table in tables_first:
            df = pd.read_html(StringIO(str(table)))[0]
            dfs_first.append(df)

        df_header = max(dfs_first, key=lambda df: df.shape[1])
        fields = df_header.columns.values

        # ------------------------------------------------------
        # SEND ALL PAGES → EXTRACT TRANSACTIONS
        # ------------------------------------------------------
        data2 = {
            "output_type": "specified-fields",
            "model": "nanonets",
            "specified_fields": ", ".join(fields)
        }

        
        files = {
            'file': open(uploaded_file, 'rb')
        }

        response_all = requests.post(url, headers=headers, files=files, data=data2)
        markdown_all = response_all.json()["content"]

        soup_other = BeautifulSoup(markdown_all, "html.parser")
        tables_other = soup_other.find_all("table")

        dfs_all = []
        for table in tables_other:
            df = pd.read_html(StringIO(str(table)))[0]
            dfs_all.append(df)

        extracted_df = pd.concat(dfs_all).reset_index(drop=True)

        end = time.time()

        st.success("Extraction Completed Successfully!")
        st.write(f"Execution Time: **{round(end - start, 2)} seconds**")

        # ------------------------------------------------------
        # DISPLAY RESULT
        # ------------------------------------------------------
        st.subheader("Extracted Transaction Data")
        st.dataframe(extracted_df)

        # ------------------------------------------------------
        # DOWNLOAD RESULT AS CSV
        # ------------------------------------------------------
        csv_data = extracted_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="⬇ Download Extracted Data (CSV)",
            data=csv_data,
            file_name="extracted_transactions.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"❌ An error occurred: {e}")
