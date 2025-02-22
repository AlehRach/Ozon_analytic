import streamlit as st
import pandas as pd
import requests as re
import plotly.express as px

from Trigger_stock import get_trigger_list
from Accruals import process_data

st.set_page_config(layout="wide")
#st.title("Ozon Data Dashboard")
# Используем session_state для управления видимостью блока ввода ключей
if "keys_entered" not in st.session_state:
    st.session_state.keys_entered = False  # По умолчанию показываем форму
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
            st.subheader("Выберите период")

            # Инициализация переменных в session_state, если их нет
            if "from_date" not in st.session_state:
                st.session_state.from_date = None
            if "to_date" not in st.session_state:
                st.session_state.to_date = None
            if "dates_selected" not in st.session_state:
                st.session_state.dates_selected = False  # Управляет отображением календаря

            # Временные переменные для хранения выбора (чтобы не перезаписывать session_state сразу)
            temp_from_date = st.date_input("Начало периода", value=st.session_state.from_date)
            temp_to_date = st.date_input("Конец периода", value=st.session_state.to_date)

            # Обновляем временные переменные, но не session_state сразу
            if temp_from_date:
                st.session_state.temp_from_date = temp_from_date
            if temp_to_date:
                st.session_state.temp_to_date = temp_to_date

            # Кнопка подтверждения появляется только если выбраны обе даты
            if "temp_from_date" in st.session_state and "temp_to_date" in st.session_state:
                if st.button("Подтвердить даты"):
                    if st.session_state.temp_from_date <= st.session_state.temp_to_date:
                        # Теперь сохраняем в session_state окончательно
                        st.session_state.from_date = st.session_state.temp_from_date.strftime("%Y-%m-%d")
                        st.session_state.to_date = st.session_state.temp_to_date.strftime("%Y-%m-%d")
                        st.session_state.dates_selected = True  # Больше не показываем календарь
                        st.rerun()
                    else:
                        st.warning("Дата начала должна быть раньше даты окончания!")

        # Отображаем выбранные даты после подтверждения
        if st.session_state.get("dates_selected", False):
            st.write(f"Выбранный период: с {st.session_state.from_date} по {st.session_state.to_date}")
            try:
                result = process_data(st.session_state.my_keys, st.session_state.from_date, st.session_state.to_date, curr_rate=105)
                if isinstance(result, str):
                    st.error(result)  # Выводим ошибку в UI
                    df_grbt, message_list = None, None
                else:
                    df_grbt, message_list = result
                st.session_state.message_list = message_list
                st.session_state.df_grbt = df_grbt
            except Exception as e:
                st.error(f'error Accruals {e}')

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