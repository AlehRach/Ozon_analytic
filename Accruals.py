import os
import sys
import pandas as pd
import numpy as np
from Get_all_goods import get_all_goods
from Trans_list import get_translist
from Table_trans import get_tabletrans

import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import io

def process_data(my_keys, d_from: str, d_to: str, curr_rate: float):
    """
    Основная функция для обработки данных и формирования итогового Excel-файла.
    
    :param d_from: Начало периода в формате 'YYYY-MM-DD'
    :param d_to: Конец периода в формате 'YYYY-MM-DD'
    :param curr_rate: Текущее значение курса валют для расчета себестоимости
    """
    google_secrets = st.secrets["google"]

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    credentials = service_account.Credentials.from_service_account_info(
        dict(google_secrets), scopes=SCOPES
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
    try:
        # 🟢 4. Загружаем `1С все.xlsx`
        files = list_drive_files(FOLDER_ID)
        xlsx_file = next((file for file in files if file["name"].endswith(".xlsx")), None)
        if xlsx_file:
            file_data = download_file(xlsx_file["id"])
            st_1c = pd.read_excel(file_data, skiprows=3)
    except Exception as e:
        return f'⚠ Ошибка загрузки файла 1С {e}'  

    profiles = ['Gr', 'Bt']

    tables_list = []
    log_list = []
 
    # Обработка каждого профиля
    for profile in profiles:
        try:
            client_id = my_keys[f"client_id_{profile}"]
            api_key = my_keys[f"api_key_{profile}"]

            if not client_id or not api_key:
                raise ValueError(f"Переменные окружения для профиля {profile} не найдены.")

            result = get_all_goods(client_id, api_key, profile)
            if isinstance(result, str):
                return result
            else:
                goods = result

            goods['Articul'] = goods['offer_id'].apply(lambda x: str(x).split('_')[0])

            result = get_translist(client_id, api_key, profile, d_from, d_to)
            if isinstance(result, str):
                return result
            else:
                trans_list_df, message_list = result

            result = get_tabletrans(trans_list_df, goods, profile)
            if isinstance(result, str):
                return result
            else:
                fin_df = result
            tables_list.append(fin_df)
            print(f'Начисления кабинета {profile} успешно сформированы')
        
        except Exception as e:
            return f"Ошибка при обработке Начисления {profile}: {e}"

    # Конкатенируем все таблицы
    df_grbt = pd.concat(tables_list, axis=0).reset_index(drop=True)

    # обработка 1С данных
    try:
        st_1c = st_1c.rename(columns={'Модель_ОЗОН': 'Articul'})

        st_1c_gr = st_1c.groupby('Articul').agg({
            'Признак ХитМП': 'first',
            'Номенклатурная Группа': 'first',
            'Ткань': 'first',
            'Сезон': 'first',
            'Цена1 (тек.)': 'first'
        }).reset_index()

        # Объединение данных
        df_grbt = df_grbt.merge(st_1c_gr, on='Articul', how='left')
        print('Данные файла 1С успешно добавленны')

    except Exception as e:
        return f"Ошибка при чтении или обработке файла Excel из 1С: {e}"

    # Переименование и реорганизация столбцов
    try:
        df_grbt = df_grbt.rename(columns={'sale_commission_r': 'комиссия_в', 'Цена1 (тек.)': 'Себестоимость'})
        df_grbt['Себестоимость'] = df_grbt['Себестоимость'] * curr_rate

        columns_to_move = ['Признак ХитМП', 'Номенклатурная Группа', 'Ткань', 'Сезон', 'Себестоимость']
        new_position = 4
        for col in columns_to_move:
            df_grbt.insert(new_position, col, df_grbt.pop(col))
            new_position += 1

        columns_to_move_yellow = ['Прямая_логистика', 'Последняя_миля']
        new_position_y = 15
        for col in columns_to_move_yellow:
            df_grbt.insert(new_position_y, col, df_grbt.pop(col))
            new_position_y += 1

        # Red block
        df_grbt['Продано_(шт)'] = df_grbt['Выкуплено_шт'] - df_grbt['Возвраты']
        df_grbt['Продано_(RUB)'] = df_grbt['Сумма_заказы'] + df_grbt['Получение_возврата']
        df_grbt['Комиссия_sum'] = df_grbt['Комиссия'] + df_grbt['комиссия_в']
        df_grbt['Логистика'] = df_grbt['Прямая_логистика'] + df_grbt['Последняя_миля'] + df_grbt['логистика_отмен'] + df_grbt['Доставка_отмена_начисления']
        df_grbt['Обработка_возвратов'] = df_grbt['обработка_возврата'] + df_grbt['отмен'] + df_grbt['невыкуп']
        df_grbt['Компенсации'] = df_grbt['Потеря_Ozon_логист'] + df_grbt['Потеря_Ozon_склад'] #+ df_grbt['Начисление_спор']
        df_grbt['Эквайринг'] = df_grbt['эквайринг']
        df_grbt['Доп_Услуги'] = df_grbt['Корр_стоимост_услуг'] + df_grbt['Трафареты'] + df_grbt['Прод_поиск'] + df_grbt['Звёздн_товары'] + df_grbt['Приобретен_отзывов'] + df_grbt['Прод_бренда'] + df_grbt['Premium_Plus'] + df_grbt['продвиж_бонус'] + df_grbt['Кросс_докинг'] + df_grbt['Обработка_грузоместа_FBO'] + df_grbt['Бронь_места_персонал_поставки_составеГМ'] + df_grbt['Опознанн_излишков_составеГМ'] + df_grbt['Утилизация_не_всрок'] + df_grbt['Утилизация_Повреждённые_упаковки'] + df_grbt['Утилизация_Повреждённые_покупателя'] + df_grbt['Утилизация_Прочее'] + df_grbt['Закреп_отзыва'] + df_grbt['Брака_приемки']
        df_grbt['К_перечислению'] = df_grbt['Продано_(RUB)'] + df_grbt['Комиссия_sum'] + df_grbt['Логистика'] + df_grbt['Обработка_возвратов']+ df_grbt['Компенсации']+ df_grbt['Эквайринг']+ df_grbt['Доп_Услуги']
        df_grbt['СС'] = df_grbt['Себестоимость'] * df_grbt['Продано_(шт)']
        df_grbt['Валовый'] = df_grbt['К_перечислению'] / 1.2 - df_grbt['СС']
        df_grbt['Маржа'] = df_grbt.apply(lambda row: (row['Валовый'] / row['К_перечислению']) * 100 if row['К_перечислению'] != 0 else np.nan, axis=1)
        df_grbt['%распределенных_затрат'] = df_grbt.apply(lambda row: ((row['Комиссия_sum'] + row['Логистика'] + row['Обработка_возвратов'] + row['Компенсации'] + row['Эквайринг'] + row['Доп_Услуги']) / row['Продано_(RUB)']) * 100 if row['Articul'] != 'неопределен' and row['Продано_(RUB)'] != 0 else np.nan, axis=1)
        df_grbt['Нераспределенные_затраты'] = df_grbt.apply(lambda row: (row['Комиссия_sum'] + row['Логистика'] + row['Обработка_возвратов'] + row['Компенсации'] + row['Эквайринг'] + row['Доп_Услуги']) if row['Articul'] == 'неопределен' else np.nan, axis=1)
        df_grbt['% выкупа'] = df_grbt.apply(lambda row: row['Выкуплено_шт'] / (row['Выкуплено_шт'] + row['Возвраты'] + row['Отмены_невыкупы']) if (row['Выкуплено_шт'] + row['Возвраты'] + row['Отмены_невыкупы']) != 0 else np.nan, axis=1)

        columns_to_move_redblock = ['Продано_(шт)', 'Продано_(RUB)', 'Комиссия_sum', 'Логистика', 'Обработка_возвратов', 'Компенсации', 'Эквайринг', 'Доп_Услуги', 'К_перечислению',
                                    'СС', 'Валовый', 'Маржа', '%распределенных_затрат', 'Нераспределенные_затраты', '% выкупа']
        new_position_red = 0
        for col in columns_to_move_redblock:
            df_grbt.insert(new_position_red, col, df_grbt.pop(col))
            new_position_red +=1
    except Exception as e:
        return f"Ошибка Переименование и реорганизация столбцов: {e}"
    
    return df_grbt, message_list
   
    # Экспорт в Excel
    # try:
    #     log_df = pd.DataFrame(log_list, columns=['Log Messages'])
    #     with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    #         workbook = writer.book
    #         worksheet = workbook.add_worksheet('Data')
    #         worksheet.write('A1', 'период')
    #         worksheet.write('B1', f'{d_from}_{d_to}')
    #         worksheet.write('A2', 'курс')
    #         worksheet.write('B2', curr_rate)
    #         df_grbt.to_excel(writer, sheet_name='Data', startrow=5, index=False)
    #         header_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
    #         for col_num, value in enumerate(df_grbt.columns.values):
    #             worksheet.write(5, col_num, value, header_format)
    #         log_df.to_excel(writer, sheet_name='Logs', index=False)
    #         red_fill = workbook.add_format({'bg_color': 'red'})
    #         worksheet.conditional_format('A5:O5', {'type': 'blanks', 'format': red_fill})
    #         yell_fill = workbook.add_format({'bg_color': 'yellow'})
    #         worksheet.conditional_format('AB5:AF5', {'type': 'blanks', 'format': yell_fill})
    #         blue_fill = workbook.add_format({'bg_color': 'blue'})
    #         worksheet.conditional_format('AG5:AR5', {'type': 'blanks', 'format': blue_fill})

    #     print(f"Данные успешно сохранены в файл: {output_path}")
    # except Exception as e:
    #     return f"Ошибка при сохранении данных в Excel: {e}"
