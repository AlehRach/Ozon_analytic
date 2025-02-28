import streamlit as st
import pandas as pd
import requests as re
import plotly.express as px
from datetime import datetime, date
import io

from Trigger_stock import get_trigger_list
from Accruals import process_data
from Googlestream import df_to_googlesheet

# Function to create an Excel file in memory
def create_excel_file(df_grbt, from_date, to_date, curr_rate, message_list):
    output = io.BytesIO()  # Create a BytesIO object to store the Excel file
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet('Data')
        
        # Write metadata (period and currency rate)
        worksheet.write('A1', 'период')
        worksheet.write('B1', f'{from_date}_{to_date}')
        worksheet.write('A2', 'курс')
        worksheet.write('B2', curr_rate)
        
        # Write the DataFrame to Excel
        df_grbt.to_excel(writer, sheet_name='Data', startrow=5, index=False)
        
        # Format the header
        header_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
        for col_num, value in enumerate(df_grbt.columns.values):
            worksheet.write(5, col_num, value, header_format)
        
        # Write log messages to a separate sheet
        log_df = pd.DataFrame(message_list, columns=['Log Messages'])
        log_df.to_excel(writer, sheet_name='Logs', index=False)
        
        # Add conditional formatting (optional)
        red_fill = workbook.add_format({'bg_color': 'red'})
        worksheet.conditional_format('A5:O5', {'type': 'blanks', 'format': red_fill})
        yell_fill = workbook.add_format({'bg_color': 'yellow'})
        worksheet.conditional_format('AB5:AF5', {'type': 'blanks', 'format': yell_fill})
        blue_fill = workbook.add_format({'bg_color': 'blue'})
        worksheet.conditional_format('AG5:AR5', {'type': 'blanks', 'format': blue_fill})
    
    output.seek(0)  # Reset the stream position to the beginning
    return output

st.set_page_config(layout="wide")
#st.title("Ozon Data Dashboard")
# Используем session_state для управления видимостью блока ввода ключей
if "keys_entered" not in st.session_state:
    st.session_state.keys_entered = False  # По умолчанию показываем форму

# Block Initialize session state variables at the start of the script
if "data_entered" not in st.session_state:
    st.session_state.data_entered = True  # Инициализируем, если атрибут отсутствует
if "saved_from_date" not in st.session_state:
    st.session_state.saved_from_date = None
if "saved_to_date" not in st.session_state:
    st.session_state.saved_to_date = None
if "saved_curr_rate" not in st.session_state:
    st.session_state.saved_curr_rate = None
if "df_grbt" not in st.session_state:
    st.session_state.df_grbt = None
if "message_list" not in st.session_state:
    st.session_state.message_list = None
# Блок ввода ключей (показывается только если keys_entered == False)
if not st.session_state.keys_entered:
    st.subheader("Введите API-ключи Ozon")
    # Input for API key
    my_keys = {}
    my_keys['client_id_Gr'] = st.text_input("Ваш Ozon client_id_Gr", type="password", key='client_id_Gr')
    my_keys['api_key_Gr'] = st.text_input("Ваш Ozon api_key_Gr", type="password", key='api_key_Gr')
    my_keys['client_id_Bt'] = st.text_input("Ваш Ozon client_id_Bt", type="password", key='client_id_Bt')
    my_keys['api_key_Bt'] = st.text_input("Ваш Ozon api_key_Bt", type="password", key='api_key_Bt')

    if st.button("Сохранить ключи и продолжить"):
        if all(my_keys.values()):  # Проверяем, что все ключи введены
            st.session_state.keys_entered = True  # Скрываем форму
            st.session_state.my_keys = my_keys  # Сохраняем ключи
            st.rerun()
        else:
            st.warning("Введите все ключи!")

# Если ключи введены, показываем основную часть дашборда
if st.session_state.keys_entered:
    col1, col2 = st.columns([1, 1])  # Разделяем пространство для двух кнопок

    with col1:

        if st.button("Получить триггерные артикулы за вчера-сегодня"):
            try:
                result = get_trigger_list(st.session_state.my_keys)

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
                st.error(f'error Triggers {e}')
  
    with col2:

        if st.button("Таблица начислений"):
            st.session_state.data_entered = False
        if not st.session_state.data_entered:
            st.subheader("Выберите период")
            from_date = st.date_input("Дата начала периода YYY-mm-dd", key='from_date')
            to_date = st.date_input("Дата окончания периода YYY-mm-dd", key='to_date')
            curr_rate = st.number_input("Текущий курс валюты", key='curr_rate')

        if st.button("Сохранить данные и продолжить"):
            if from_date and to_date and curr_rate is not None:
                st.session_state.data_entered = True  # Hide the form
                st.session_state.saved_from_date = from_date
                st.session_state.saved_to_date = to_date
                st.session_state.saved_curr_rate = curr_rate
                st.rerun()
            else:
                st.warning("Введите все данные!")                  
        if st.session_state.data_entered:
            try:
                from_date_str = st.session_state.saved_from_date.strftime('%Y-%m-%d')
                to_date_str = st.session_state.saved_to_date.strftime('%Y-%m-%d')
                st.write(f'start period- {from_date_str}, end- {to_date_str}, curr rate- {st.session_state.saved_curr_rate}')
                result = process_data(st.session_state.my_keys, from_date_str, to_date_str, st.session_state.saved_curr_rate)
                if isinstance(result, str):
                    st.error(result)  # Выводим ошибку в UI
                    df_grbt, message_list = None, None
                else:
                    df_grbt, message_list = result
                st.session_state.message_list = message_list
                st.session_state.df_grbt = df_grbt
            except Exception as e:
                st.error(f'error Accruals {e}')

# Block- Таблица начислений
if st.session_state.data_entered and st.session_state.df_grbt is not None:
    st.write('## Таблица начислений')
    st.data_editor(st.session_state.df_grbt)
    if st.session_state.message_list:
        st.write(st.session_state.message_list)

    # Add a download button for Excel export
    if st.button("Сохранить в Google Sheets"):
        try:
            # Create the Google Sheets in memory
            df_to_googlesheet(df_grbt, message_list, from_date_str, to_date_str)
        except Exception as e:
            st.error(f"Ошибка при создании Google Sheets: {e}")
else:
    pass

# Check if data is loaded
if 'df' in st.session_state and 'df_details' in st.session_state:
    df = st.session_state.df
    df_details = st.session_state.df_details

    col1, col2 = st.columns([1.1, 1.8])  # col1 - для таблицы и выпадающего списка, col2 - для графика
    with col1:
        st.write("### Triggered articuls")
        st.write(df)

        # Dropdown to select an articul
        articul_list = df['articul'].unique()
        selected_articul = st.selectbox("Select an Articul", articul_list)

    with col2:
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