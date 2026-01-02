from collections import defaultdict
import datetime
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

# --- 1. Database Configuration ---
DB_USER = 'airflow'
DB_PASS = 'airflow'
DB_HOST = 'postgres-instance'
DB_PORT = '5432'
DB_NAME = 'airflow'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 2. Helper Functions ---

@st.cache_resource
def get_engine():
    """Create a database connection engine."""
    return create_engine(DATABASE_URL)

@st.cache_data(ttl=3600) # Cache this longer (1 hour) as cities rarely change
def get_filter_options():
    engine = get_engine()
    query = """
    SELECT DISTINCT 
        city_name, 
        postal_code, 
        department_num 
    FROM DIM_BUILDING 
    ORDER BY city_name, department_num
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=600)
def load_metric_data(metric_column, date_range, gas_label, energy_label, regions, cities, postal_codes, revenue_class):
    """
    Generic function to load data for a SINGLE metric.
    Filters out rows where this specific metric is NULL.
    """
    engine = get_engine()

    # We use an f-string for the column name (metric_column).
    # NOTE: Only use f-strings for internal identifiers (table/column names). 
    # Use %(params)s for values (dates, labels) to prevent SQL Injection.
    query = f"""
    SELECT 
        d.year,
        d.month,
        d.day,
        d.full_date,
        b.gas_emissions_label,
        b.energy_consumption_label,
        r.class,
        SUM(f.{metric_column}) as total_value,  -- Dynamic Column
        COUNT(*) as valid_count               -- Count of non-null rows for this metric
    FROM 
        FACT_TABLE f
        INNER JOIN DIM_DATE d ON f.date_id = d.date_id
        INNER JOIN DIM_BUILDING b ON f.building_id = b.building_id
        INNER JOIN DIM_REVENUE r ON f.revenue_id = r.revenue_id
    WHERE 
        1=1
        AND f.{metric_column} IS NOT NULL     -- Dynamic Filter
    """
    params= defaultdict(str)

    # Dynamic Filtering for Dimensions
    if date_range is not None:
        # Check if user selected both start and end dates (st.date_input returns a tuple)
        if len(date_range) == 2:
            start_date, end_date = date_range
            query += " AND d.full_date >= %(start_date)s AND d.full_date <= %(end_date)s"
            params['start_date'] = start_date
            params['end_date'] = end_date

    if gas_label != "All":
        query += " AND b.gas_emissions_label = %(gas_label)s"
        params['gas_label'] = gas_label

    if energy_label != "All":
        query += " AND b.energy_consumption_label = %(energy_label)s"
        params['energy_label'] = energy_label

    if regions:
        query += " AND b.department_num IN %(regions)s"
        params['regions'] = tuple(regions)

    if cities:
        query += " AND b.city_name IN %(cities)s"
        params['cities'] = tuple(cities)

    if postal_codes:
        query += " AND b.postal_code IN %(postal_codes)s"
        params['postal_codes'] = tuple(postal_codes)
    
    if revenue_class != "All":
        query += " AND r.class = %(revenue_class)s"
        params['revenue_class'] = revenue_class

    # Grouping
    query += """
    GROUP BY 
        d.year, d.month, d.day, d.full_date,
        b.gas_emissions_label, b.energy_consumption_label,
        r.class
    ORDER BY 
        d.year ASC, d.month ASC, d.day ASC
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
        
    return df

def analysis(c1, c2, c3, c4, df, gas_label="All", energy_label="All", revenue_class="All"):

    def get_weighted_avg(sub_df, group_col):
        # 1. Group by the specific column (e.g., 'gas_emissions_label')
        grouped = sub_df.groupby(group_col)[['total_value', 'valid_count']].sum()
        
        # 2. Calculate average: Sum of Values / Sum of Counts
        return grouped['total_value'] / grouped['valid_count']
    
    with c1:
        st.subheader("Over Time")
        chart_data = get_weighted_avg(df, 'full_date')
        st.line_chart(chart_data)

    with c2:
        st.subheader("By Gas Label")
        if gas_label == "All":
            bar_data = get_weighted_avg(df, 'gas_emissions_label')
            st.bar_chart(bar_data)
        else:
            st.info("Select 'All' to see comparison.")

    with c3:
        st.subheader("By Energy Label")
        if energy_label == "All":
            bar_data = get_weighted_avg(df, 'energy_consumption_label')
            st.bar_chart(bar_data)
        else:
            st.info("Select 'All' to see comparison.")

    with c4:
        st.subheader("By Revenue Class")
        if revenue_class == "All":
            bar_data = get_weighted_avg(df, 'class')
            st.bar_chart(bar_data)
        else:
            st.info("Select 'All' to see comparison.")


# --- 3. Streamlit Layout ---

st.set_page_config(page_title="City Vibes Dashboard", layout="wide")
st.title("📊 City Vibes Dashboard")

# Sidebar: Filters
df_options = get_filter_options()

st.sidebar.header("Filters")

all_zips = df_options['postal_code'].dropna().unique().tolist()
selected_zips = st.sidebar.multiselect("Select Postal Code(s)", options=all_zips)

all_regions = df_options['department_num'].dropna().unique().tolist()
selected_regions = st.sidebar.multiselect("Select Department(s)", options=all_regions)

all_cities = df_options['city_name'].dropna().unique().tolist()
selected_cities = st.sidebar.multiselect("Select City(s)", options=all_cities)

date_mode = st.selectbox(
    "Choose a Time Period",
    options=["Custom Range", "All Time (Forever)"],
    index=1  # Default to Custom
)

today = datetime.date.today()

if date_mode == "All Time (Forever)":
    # Pass None to trigger the "WHERE 1=1" logic (No date filter)
    d = None
    st.caption("Showing all data available in the database.")
else: # "Custom Range"
    d = st.date_input(
        "Select specific dates",
        (datetime.date(2023, 1, 1), datetime.date(2024, 1, 1)),
        format="MM.DD.YYYY"
    )
gas_label = st.sidebar.selectbox("Gas Emission", ["All", "A", "B", "C", "D", "E", "F", "G"])
energy_label = st.sidebar.selectbox("Energy Consumption", ["All", "A", "B", "C", "D", "E", "F", "G"])
revenue_class = st.sidebar.selectbox("Revenue Class", ["All", "A", "B", "C", "D", "E"])

try:
    # --- 4. Loading Data Separately ---
    # We load 4 separate DataFrames. This allows 'Land Surface' to have 500 rows 
    # even if 'Price m2' only has 400 rows.
    
    with st.spinner("Loading metrics..."):
        df_trans = load_metric_data('transaction_value', d, gas_label, energy_label, selected_regions, selected_cities, selected_zips, revenue_class)
        df_price = load_metric_data('price_m2', d, gas_label, energy_label, selected_regions, selected_cities, selected_zips, revenue_class)
        df_land  = load_metric_data('land_surface', d, gas_label, energy_label, selected_regions, selected_cities, selected_zips, revenue_class)
        df_built = load_metric_data('built_surface', d, gas_label, energy_label, selected_regions, selected_cities, selected_zips, revenue_class)

    # Check if primary data exists
    if df_trans.empty:
        st.warning("No transaction data found for these filters.")
    else:
        # --- 5. KPIs Display ---
        st.markdown("### Data Availability & Averages")
        
        cols = st.columns(4)
        
        # Helper to safely calculate stats
        def display_metric(col, title, df, format_str):
            if df.empty:
                col.metric(title, "No Data")
            else:
                total_sum = df['total_value'].sum() 
                total_count = df['valid_count'].sum()
                
                if total_count == 0:
                    real_avg = 0
                else:
                    real_avg = total_sum / total_count
                
                col.metric(f"{title}", f"{real_avg:{format_str}}")
                col.caption(f"Based on {total_count:,} records")

        # Display Metrics with their specific counts
        display_metric(cols[0], "Avg Transaction", df_trans, ",.2f")
        display_metric(cols[1], "Avg Price m²", df_price, ",.2f")
        display_metric(cols[2], "Avg Land Surf", df_land, ",.2f")
        display_metric(cols[3], "Avg Built Surf", df_built, ",.2f")

        st.markdown("---")

        # --- 6. Visualizations ---
        # We use df_trans for the main charts as they focus on Transaction Value
        st.header("Count Analysis")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.subheader("Over Time")
            chart_data = df_trans.groupby('full_date')['valid_count'].sum()
            st.line_chart(chart_data)

        with c2:
            st.subheader("By Gas Label")
            if gas_label == "All": 
                bar_data = df_trans.groupby('gas_emissions_label')['valid_count'].sum()
                st.bar_chart(bar_data)
            else:
                st.info("Select 'All' to see comparison.")

        with c3:
            st.subheader("By Energy Label")
            if energy_label == "All":
                bar_data = df_trans.groupby('energy_consumption_label')['valid_count'].sum()
                st.bar_chart(bar_data)
            else:
                st.info("Select 'All' to see comparison.")
        
        with c4:
            st.subheader("By Revenue Class")
            if revenue_class == "All":
                bar_data = df_trans.groupby('class')['valid_count'].sum()
                st.bar_chart(bar_data)
            else:
                st.info("Select 'All' to see comparison.")
        
        st.markdown("---")

        st.header("Value Analysis")

        st.subheader("Transaction Value Analysis")
        c1, c2, c3, c4 = st.columns(4)
        analysis(c1, c2, c3, c4, df_trans, gas_label, energy_label, revenue_class)

        st.markdown("---")

        st.subheader("Price per m² Analysis")
        c1, c2, c3, c4 = st.columns(4)
        analysis(c1, c2, c3, c4, df_price, gas_label, energy_label, revenue_class)

        st.markdown("---") 

        st.subheader("Land Surface Analysis")
        c1, c2, c3, c4 = st.columns(4)
        analysis(c1, c2, c3, c4, df_land, gas_label, energy_label, revenue_class)

        st.markdown("---")

        st.subheader("Built Surface Analysis")
        c1, c2, c3, c4 = st.columns(4)
        analysis(c1, c2, c3, c4, df_built, gas_label, energy_label, revenue_class)  
        

        # Tabs to view raw data for each metric
        st.subheader("Raw Data Inspector")
        tab1, tab2, tab3, tab4 = st.tabs(["Transactions", "Price m²", "Land", "Built"])

        df_trans['avg_transaction_value'] = df_trans['total_value'] / df_trans['valid_count']
        tab1.dataframe(df_trans)
        df_price['avg_price_m2'] = df_price['total_value'] / df_price['valid_count']
        tab2.dataframe(df_price)
        df_land['avg_land_surface'] = df_land['total_value'] / df_land['valid_count']
        tab3.dataframe(df_land)
        df_built['avg_built_surface'] = df_built['total_value'] / df_built['valid_count']
        tab4.dataframe(df_built)

except Exception as e:
    st.error(f"Error: {e}")