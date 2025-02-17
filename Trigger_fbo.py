import requests as re
import pandas as pd
import time
from datetime import datetime, timedelta

def get_trigger_orders(my_keys):

    url_shipments = 'https://api-seller.ozon.ru/v2/posting/fbo/list'

    def split_date_range(start_date, end_date, delta_days):
        dates = []
        current_date = start_date
        while current_date < end_date:
            next_date = current_date + timedelta(days=delta_days)
            dates.append((current_date, min(next_date, end_date)))
            current_date = next_date
        return dates

    end_date = datetime.now()
    start_date = end_date - timedelta(days=14)
    date_ranges = split_date_range(start_date, end_date, delta_days=7)
    list_data = []

    profiles = ['Gr', 'Bt']

    for profile in profiles:
        client_id = my_keys[f'client_id_{profile}']
        api_key = my_keys[f'api_key_{profile}']

        headers = {'Client-Id': client_id, 'Api-Key': api_key}

        for date_range in date_ranges:
            since = date_range[0].strftime('%Y-%m-%dT%H:%M:%S.0Z')
            to = date_range[1].strftime('%Y-%m-%dT%H:%M:%S.0Z')
            offset = 0

            while True:
                params = {'filter': {'since': since, 'to': to},
                        'limit': 1000, 'offset': offset,
                        'with': {'analytics_data': True, 'financial_data': True}}
                try:
                    res = re.post(url_shipments, headers=headers, json=params)
                    res.raise_for_status()
                    ship_list = res.json()
                    list_data.extend(ship_list['result'])
                    if len(ship_list['result']) < 1000:
                        break
                    offset += 1000
                    print(f'got data up to {to} for profile {profile}')
                except Exception as e:
                    return f'error parsing {e} with page {offset} in profile: {profile}'
                time.sleep(2)
    try:
        orders = pd.json_normalize(list_data)

        # Распаковываем вложенные данные о продуктах
        products = pd.json_normalize(list_data, record_path=['products'], meta=['order_id'])

        # Распаковываем финансовые данные о продуктах
        financial_products = pd.json_normalize(
            list_data, 
            record_path=['financial_data', 'products'], 
            meta=['order_id']
        )
        columns_to_drop = [col for col in orders.columns if "financial_data" in col]
        orders_cleaned = orders.drop(columns=columns_to_drop + ['products', 'additional_data'])

        fin_df = orders_cleaned.merge(products, on='order_id', how='left')
        fin_df = fin_df.merge(financial_products, on='order_id', how='left')

        fin_df['created_at'] = pd.to_datetime(fin_df['created_at']).dt.date
        fin_df['created_at'] = pd.to_datetime(fin_df['created_at'])

        fin_df_sort = fin_df.groupby('order_id').agg({'created_at': 'first', 'offer_id': 'first', 'quantity': 'first', 'price_x': 'first', 'actions': 'first'}).reset_index()
        ffd_group = fin_df_sort.groupby(['created_at', 'offer_id']).agg({'quantity': 'sum', 'price_x': 'first', 'actions': 'first'}).reset_index()

        ffd_group = ffd_group.rename(columns={'created_at': 'Day'})

        ffd_group['articul'] = ffd_group['offer_id'].apply(lambda x: x.split('_')[0])
        orders_df = ffd_group.groupby(['Day', 'articul'])['quantity'].sum().reset_index()
    except Exception as e:
        return f'error dataframe {e}'
    return orders_df