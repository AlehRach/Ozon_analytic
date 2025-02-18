import streamlit as st
import pandas as pd
import requests as re
import plotly.express as px

from Trigger_stock import get_trigger_list

st.title("Ozon Data Dashboard")
# Input for API key
my_keys = {}
my_keys['client_id_Gr'] = st.text_input("Enter your Ozon client_id_Gr", type="password", key='client_id_Gr')
my_keys['api_key_Gr'] = st.text_input("Enter your Ozon api_key_Gr", type="password", key='api_key_Gr')
my_keys['client_id_Bt'] = st.text_input("Enter your Ozon client_id_Bt", type="password", key='client_id_Bt')
my_keys['api_key_Bt'] = st.text_input("Enter your Ozon api_key_Bt", type="password", key='api_key_Bt')
if st.button("Fetch and Process Data"):
    try:
        result = get_trigger_list(my_keys)

# Проверяем, является ли результат ошибкой (строкой)
        if isinstance(result, str):
            st.error(result)  # Выводим ошибку в UI
            df_details, df, combined_list = None, None, None  # Устанавливаем None, чтобы избежать ошибок
        else:
            df_details, df, combined_list = result

        st.session_state.df_details = df_details
        st.session_state.df = df
        st.session_state.fig = None  # Очистка старого графика
    except Exception as e:
        st.error(f'error Trigger {e}')

# Check if data is loaded
if 'df' in st.session_state and 'df_details' in st.session_state:
    df = st.session_state.df
    df_details = st.session_state.df_details

    st.write("### Triggered articuls")
    st.write(df)

    # Dropdown to select an articul
    articul_list = df['articul'].unique()
    selected_articul = st.selectbox("Select an Articul", articul_list)

    # Filter df_details for the selected articul
    filtered_details = df_details[df_details['articul'] == selected_articul]

    # Plot the line graph
    if not filtered_details.empty:
        st.write(f"### Stock Data for Articul: {selected_articul}")

        # Create the line graph
        fig = px.line(filtered_details, x='Day', y='stocks_present', title=f"Stock Data for {selected_articul}")

        # Add dots for flag_stock
        flag_stock_points = filtered_details[filtered_details['flag_stock'] == 1]
        fig.add_scatter(x=flag_stock_points['Day'], y=flag_stock_points['stocks_present'], mode='markers', name='Распродается', marker=dict(color='red'))
        flag_out_points = filtered_details[filtered_details['flag_out'] == 1]
        fig.add_scatter(x=flag_out_points['Day'], y=flag_out_points['stocks_present'], mode='markers', name='Вымывается', marker=dict(color='orange'))

        # Display the graph
        st.plotly_chart(fig)
    else:
        st.warning(f"No data found for articul: {selected_articul}")
else:
    st.info("Click 'Fetch and Process Data' to load the data.")