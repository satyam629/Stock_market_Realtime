# Import libraries
import altair as alt
import streamlit as st
import pandas as pd
from datetime import timedelta
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col, when, max, lag
from snowflake.snowpark import Window

# Set page configuration for Streamlit
st.set_page_config(layout="wide")

# Establish Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()

# Cache data loading function to avoid re-fetching each time the app reloads
@st.cache_data()
def load_data():
    """
    Loads stock price data and FX rates data from Snowflake, performs transformations,
    and returns two pandas DataFrames.
    """
    # Query to load and transform stock price data
    snow_df_stocks = (
        session.table("FINANCIALS__ECONOMICS_ENTERPRISE.CYBERSYN.STOCK_PRICE_TIMESERIES")
        .filter(
            (col('TICKER').isin('AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA')) & 
            (col('VARIABLE_NAME').isin('Nasdaq Volume', 'Post-Market Close')))
        .groupBy("TICKER", "DATE")
        .agg(
            max(when(col("VARIABLE_NAME") == "Nasdaq Volume", col("VALUE"))).alias("NASDAQ_VOLUME"),
            max(when(col("VARIABLE_NAME") == "Post-Market Close", col("VALUE"))).alias("POSTMARKET_CLOSE")
        )
    )

    # Day over Day Post-market Close Change calculation using Window function
    window_spec = Window.partitionBy("TICKER").orderBy("DATE")
    snow_df_stocks_transformed = snow_df_stocks.withColumn(
        "DAY_OVER_DAY_CHANGE", 
        (col("POSTMARKET_CLOSE") - lag(col("POSTMARKET_CLOSE"), 1).over(window_spec)) /
        lag(col("POSTMARKET_CLOSE"), 1).over(window_spec)
    )

    # Query to load foreign exchange (FX) rates data for EUR
    snow_df_fx = session.table("FINANCIALS__ECONOMICS_ENTERPRISE.CYBERSYN.FX_RATES_TIMESERIES").filter(
        (col('BASE_CURRENCY_ID') == 'EUR') & (col('DATE') >= '2019-01-01')
    ).with_column_renamed('VARIABLE_NAME', 'EXCHANGE_RATE')

    return snow_df_stocks_transformed.to_pandas(), snow_df_fx.to_pandas()

# Load the stock and FX data
df_stocks, df_fx = load_data()

def stock_prices():
    """
    Display stock performance for selected companies over a specified date range.
    """
    st.subheader('Stock Performance on the Nasdaq for the Magnificent 7')

    # Convert 'DATE' to datetime and calculate date limits
    df_stocks['DATE'] = pd.to_datetime(df_stocks['DATE'])
    max_date = df_stocks['DATE'].max()
    min_date = df_stocks['DATE'].min()

    # Set default date range (last 30 days) for date picker
    default_start_date = max_date - timedelta(days=30)
    start_date, end_date = st.date_input(
        "Date range:", 
        [default_start_date, max_date], 
        min_value=min_date, 
        max_value=max_date, 
        key='date_range'
    )

    # Filter stock data based on selected date range
    df_filtered = df_stocks[(df_stocks['DATE'] >= pd.to_datetime(start_date)) & 
                            (df_stocks['DATE'] <= pd.to_datetime(end_date))]

    # Ticker multi-select filter with default tickers
    unique_tickers = df_filtered['TICKER'].unique().tolist()
    default_tickers = [ticker for ticker in ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA'] 
                       if ticker in unique_tickers]
    selected_tickers = st.multiselect('Ticker(s):', unique_tickers, default=default_tickers)
    df_filtered = df_filtered[df_filtered['TICKER'].isin(selected_tickers)]

    # Metric selection with default as 'DAY_OVER_DAY_CHANGE'
    metric = st.selectbox('Metric:', ('DAY_OVER_DAY_CHANGE', 'POSTMARKET_CLOSE', 'NASDAQ_VOLUME'), index=0)

    # Display Altair line chart for selected tickers and metric
    line_chart = alt.Chart(df_filtered).mark_line().encode(
        x='DATE:T',
        y=alt.Y(metric, title=metric),
        color='TICKER:N',
        tooltip=['TICKER:N', 'DATE:T', metric]
    ).interactive()

    st.altair_chart(line_chart, use_container_width=True)

def fx_rates():
    """
    Display EUR foreign exchange rates for selected currencies over time.
    """
    st.subheader('EUR Exchange (FX) Rates by Currency Over Time')

    # List of currencies and default selection
    currencies = ['British Pound Sterling', 'Canadian Dollar', 'United States Dollar', 'Japanese Yen', 
                  'Polish Zloty', 'Turkish Lira', 'Swiss Franc']
    selected_currencies = st.multiselect('', currencies, 
                                         default=['British Pound Sterling', 'Canadian Dollar', 
                                                  'United States Dollar', 'Swiss Franc', 'Polish Zloty'])

    # Filter FX data based on selected currencies
    df_fx_filtered = df_fx[df_fx['QUOTE_CURRENCY_NAME'].isin(selected_currencies)]

    # Display Altair line chart for FX rates over time
    line_chart = alt.Chart(df_fx_filtered).mark_line().encode(
        x='DATE:T',
        y='VALUE:Q',
        color='QUOTE_CURRENCY_NAME:N',
        tooltip=['QUOTE_CURRENCY_NAME:N', 'DATE:T', 'VALUE:Q']
    ).interactive()

    st.altair_chart(line_chart, use_container_width=True)

# Display main header
st.header("Cybersyn: Financial & Economic Enterprise")

# Sidebar for page selection
page_names_to_funcs = {
    "Daily Stock Performance Data": stock_prices,
    "Exchange (FX) Rates": fx_rates
}
selected_page = st.sidebar.selectbox("Select a page:", page_names_to_funcs.keys())

# Load the selected page function
page_names_to_funcs[selected_page]()
