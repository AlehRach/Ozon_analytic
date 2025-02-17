import pandas as pd
import os
import time
from datetime import datetime, timedelta
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

import concurrent.futures
from Trigger_fbo import get_trigger_orders

import streamlit as st
import json

def get_trigger_list(my_keys):
    # Авторизация
    # Получение секрета из Streamlit
    google_secrets = st.secrets["google"]
    SERVICE_ACCOUNT_JSON = google_secrets["SERVICE_ACCOUNT_FILE"]
    credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    credentials = service_account.Credentials.from_service_account_file(
        credentials_dict, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=credentials)
    FOLDER_ID = '1RdrpiKMbhNacsrs6kZCxpsSwphbzhUW7'

    # 🔹 Функция получения списка файлов в папке Google Drive
    def list_drive_files(folder_id):
        results = drive_service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)").execute()
        return results.get("files", [])

    # 🔹 Функция скачивания файла
    def download_file(file_id):
        request = drive_service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file.seek(0)
        return file

    # 🟢 Запускаем get_trigger_orders() в фоне, пока файлы загружаются и обрабатываются
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_orders = executor.submit(get_trigger_orders, my_keys)  # Запуск в фоне
        # 🟢 1. Получаем список файлов в папке Google Drive
        files = list_drive_files(FOLDER_ID)

        # 🟢 2. Фильтруем файлы по названию
        dfs = []
        current_date = datetime.now()

        for file in files:
            filename = file["name"]
            file_id = file["id"]

            if 'Bt' in filename or 'Gr' in filename:
                try:
                    # Извлекаем дату из имени файла
                    date_str = filename.split('_')[1].split('.')[0]
                    file_date = datetime.strptime(date_str, '%Y-%m-%d')

                    # Проверяем, подходит ли дата
                    if current_date - timedelta(days=14) <= file_date <= current_date:
                        print(f"🔹 Загружаем: {filename}")

                        # Загружаем CSV
                        file_data = download_file(file_id)
                        df = pd.read_csv(file_data)

                        # Добавляем столбец "Day"
                        df["Day"] = file_date
                        column_list = ['Day', 'id', 'offer_id', 'name', 'marketing_price', 'price', 'old_price', 'stocks_present', 'stocks_reserved']
                        df = df[column_list]

                        # Добавляем в список
                        dfs.append(df)
                except Exception as e:
                    return f"⚠ Ошибка обработки {filename}: {e}"

        # 🟢 3. Объединяем все DataFrame в один
        if dfs:
            combined_df = pd.concat(dfs, axis=0)
        else:
            print("⚠ Нет подходящих файлов!")
            exit()

        # 🟢 4. Загружаем `1С все.xlsx`
        xlsx_file = next((file for file in files if file["name"].endswith(".xlsx")), None)
        if xlsx_file:
            file_data = download_file(xlsx_file["id"])
            df1c = pd.read_excel(file_data, skiprows=3)

            # Фильтруем по "Признак ХитМП"
            df1c = df1c[pd.notna(df1c["Признак ХитМП"])]
            hitmp = df1c["Арт_ОЗОН"].unique()

            # 🟢 5. Фильтруем по `offer_id`
            combined_df = combined_df[combined_df["offer_id"].isin(hitmp)]
            print("✅ Данные загружены и отфильтрованы!")
        else:
            return "⚠ Файл 29.01.25 все.xlsx не найден!"

        grouped_df = combined_df.groupby(['offer_id', 'Day'])['stocks_present'].sum().reset_index()

        grouped_df['articul'] = grouped_df['offer_id'].apply(lambda x: x.split('_')[0])
        grouped_df = grouped_df.groupby(['Day', 'articul'])['stocks_present'].sum().reset_index()

        # Получение заказов ФБО/ Ожидаем завершения get_trigger_orders()
        orders_df = future_orders.result()
        if isinstance(orders_df, str):  # Если функция вернула строку (ошибку)
            return orders_df
        
    grouped_df = grouped_df.merge(orders_df, on=['Day', 'articul'], how='left')
    grouped_df['quantity'] = grouped_df['quantity'].fillna(0)

    grouped_df['diff'] = grouped_df.groupby('articul')['stocks_present'].diff()
    grouped_df['diff'] = grouped_df['diff'].apply(lambda x: 0 if x>0 else x)
    grouped_df['roll_mean4'] = grouped_df.groupby('articul')['quantity'].apply(lambda x: x.rolling(window=4, min_periods=3).mean()).reset_index(level=0, drop=True)

    grouped_df['stuck'] = 0
    for i in range(len(grouped_df)-6):
        if (grouped_df['diff'].iloc[i: i+7]>=0).all() and (grouped_df['stocks_present'].iloc[i: i+7] > 10).all():
            grouped_df.loc[i+6, 'stuck'] = 1

    grouped_df['flag_stock'] =  grouped_df.apply(lambda row: 1 if row['stocks_present']>1 and row['roll_mean4']>=3 and (row['quantity'] / row['roll_mean4'])>=1.5 else 0, axis=1)

    grouped_df['flag_out'] =  grouped_df.apply(lambda row: 1 if row['stocks_present']>1 and (row['stocks_present'] - 7 * row['roll_mean4']) <=1 else 0, axis=1)

    delta_day1 = current_date - timedelta(days=1)
    delta_day2 = current_date - timedelta(days=14)
    from_day = delta_day1.strftime('%Y-%m-%d')
    graf_day = delta_day2.strftime('%Y-%m-%d')

    list_stock = []
    list_stock = grouped_df[(grouped_df['flag_stock']==1) & (grouped_df['stocks_present']!=0) & (grouped_df['Day']>=from_day)]['articul'].unique()
    list_out = grouped_df[(grouped_df['flag_out']==1) & (grouped_df['stocks_present']!=0) & (grouped_df['Day']>=from_day)]['articul'].unique()
    combined_list = list(set(list_stock).union(set(list_out)))

    list_stock_df = grouped_df[((grouped_df['flag_stock']==1) | (grouped_df['flag_out']==1)) & (grouped_df['Day']>=from_day)].copy()
    list_stock_df['Day'] = list_stock_df['Day'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_details = grouped_df[(grouped_df['articul'].isin(combined_list)) & (grouped_df['Day']>=graf_day)].copy()
    df_details['Day'] = df_details['Day'].apply(lambda x: x.strftime('%Y-%m-%d'))

    list_stock_df.fillna(0, inplace=True)
    df_details.fillna(0, inplace=True)

    print(combined_list)
    return df_details, list_stock_df, combined_list