import streamlit as st
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd

def df_to_googlesheet(df, message_list, d_from, d_to):

    google_secrets = st.secrets["google"]

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    credentials = service_account.Credentials.from_service_account_info(
        dict(google_secrets), scopes=SCOPES)
    
    client = gspread.authorize(credentials)

    # Название таблицы
    spreadsheet_name = f'Начисления_{d_from}_{d_to}'
    spreadsheet = client.create(spreadsheet_name)

    folder_id = '15LLtt1Ae3lI_JpxIvrIF-rhJXe6-_ohB'
    drive_service = build('drive', 'v3', credentials=credentials)
    drive_service.files().update(fileId=spreadsheet.id, addParents=folder_id, removeParents='root').execute()

    # Доступ для коллег (добавь их email)
    spreadsheet.share('Marandsor@gmail.com', perm_type='user', role='writer')
    spreadsheet.share('tchingalaev@gmail.com', perm_type='user', role='writer')

    sheet = spreadsheet.sheet1
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    try:
        details_sheet = spreadsheet.worksheet('Изменения')
    except gspread.WorksheetNotFound:
        details_sheet = spreadsheet.add_worksheet(title='Изменения', rows=100, cols=20)
    details_sheet.update('A1', [[msg] for msg in message_list])
    st.write(f"Данные успешно загружены в Google Sheets: [Открыть таблицу]({spreadsheet.url})")

    