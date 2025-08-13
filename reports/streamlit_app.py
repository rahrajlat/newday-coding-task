import pandas as pd
import streamlit as st
from utils.db_query_utils import DbQueryTool

db_postgress = DbQueryTool(
    host='localhost',
    port=5432,
    user="admin",
    password="admin",
    dbname="dbt_db",
    db_type="Postgress",
)

st.set_page_config(page_title="Monthly Revenue Dashboard", layout="wide")

# --- Streamlit App ---


# Dropdown for category
category = st.selectbox("Select Category", ['Question 1'])


if category == 'Question 1':
    st.title("Monthly Revenue Dashboard")
    st.info(
        """

## Question 1:
Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.

    """



    )
    total_revenue_by_product_category = db_postgress.execute_sql(
        sql="select * from marts.mart_sales_revenue_product_category_monthly")

    st.dataframe(total_revenue_by_product_category)

    st.bar_chart(
        total_revenue_by_product_category.set_index(
            "monthly_date")[["category", "net_revenue_incl_shipping"]],
        use_container_width=True
    )


elif category == 'Question 2':

    st.title("Monthly Revenue Dashboard")
    st.info(
        """

## Question 2:
Create a dbt model that calculates total revenue by product category for each month. Include basic data transformations and aggregations.

    """



    )
