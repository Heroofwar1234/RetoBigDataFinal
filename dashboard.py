import streamlit as st
import pandas as pd
import json

# Load the processed data
DATA_PATH = "results/data.json"

@st.cache_data
def load_data():
    with open(DATA_PATH, "r") as file:
        data = json.load(file)
    return pd.DataFrame([json.loads(row) for row in data])

# Streamlit App Layout
st.set_page_config(page_title="People Data Dashboard", layout="wide")

st.title("People Data Dashboard 📊")

# Load and display data
try:
    df = load_data()
    st.write("### Filtered Data from `people.csv`")
    st.dataframe(df)

    # Birth year filter
    min_year, max_year = int(df["birth"].min()[:4]), int(df["birth"].max()[:4])
    selected_year = st.slider("Select Birth Year", min_year, max_year, min_year)

    filtered_df = df[df["birth"].str.startswith(str(selected_year))]
    st.write(f"### People Born in {selected_year}")
    st.dataframe(filtered_df)

    # Sex distribution
    st.write("### Gender Distribution (1903-1911)")
    gender_counts = df["sex"].value_counts()
    st.bar_chart(gender_counts)

except FileNotFoundError:
    st.error(f"Processed data `{DATA_PATH}` not found! Run `people.py` first.")

