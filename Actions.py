import requests as re
import pandas as pd
import concurrent.futures
import time

from Get_all_goods import get_all_goods

def get_actions(my_keys):
    url_actionlist = 'https://api-seller.ozon.ru/v1/actions'
    profiles = ['Gr', 'Bt']
    df_list = []

    for profile in profiles:
        client_id = my_keys[f'client_id_{profile}']
        api_key = my_keys[f'api_key_{profile}']

        headers = {'Client-Id': client_id, 'Api-Key': api_key}

        try:
            res = re.get(url_actionlist, headers=headers)
            res.raise_for_status()
            df_actionlist = pd.json_normalize(res.json()['result'])
        except Exception as e:
            return f'error parsing action_list {e} in profile: {profile}'
        df_actionlist = df_actionlist.rename(columns={'id': 'action_id'})
        
        url_actioncands = 'https://api-seller.ozon.ru/v1/actions/candidates'
        url_executegoods = 'https://api-seller.ozon.ru/v1/actions/products'
        
        def get_actions_data(headers, url, df_actionlist):

            action_goods = []

            for action_id in df_actionlist['action_id'].unique():
                offset = 0
                while True:
                    try:
                        params = {'action_id': int(action_id), 'limit': 100, 'offset': offset}
                        res = re.post(url, headers=headers, json=params)
                        res.raise_for_status()
                        action = res.json()['result']['products']

                        for product in action:
                            product['action_id'] = action_id

                        action_goods.extend(action)
                        if len(action) < 100:
                            break
                        else:
                            offset+=100
                    except Exception as e:
                        return f'error get_cands{e}'
                    time.sleep(3)
            df_action_goods = pd.json_normalize(action_goods)
            return df_action_goods


             
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_candsaction = executor.submit(get_actions_data, headers, url_actioncands, df_actionlist)

            result = get_all_goods(client_id, api_key, profile)
            if isinstance(result, str):
                return result
                df_goods = None  # Устанавливаем None, чтобы избежать ошибок
            else:
                df_goods = result

            result = get_actions_data(headers, url_executegoods, df_actionlist)
            if isinstance(result, str):
                return result
                df_inaction = None  # Устанавливаем None, чтобы избежать ошибок
            else:
                df_inaction = result

            df_candsaction = future_candsaction.result()
            if isinstance(df_candsaction, str):
                return df_candsaction

        try:    
            df_candsaction = df_candsaction.merge(df_actionlist[['action_id', 'date_start', 'date_end', 'title']], on='action_id', how='left')
            df_candsaction['В_акции'] = "нет"
            df_inaction = df_inaction.merge(df_actionlist[['action_id', 'date_start', 'date_end', 'title']], on='action_id', how='left')
            df_inaction['В_акции'] = "да"
            df_in = df_inaction.merge(df_goods[['id', 'offer_id', 'name', 'stocks_present']], on='id', how='left')
            df_cand = df_candsaction.merge(df_goods[['id', 'offer_id', 'name', 'stocks_present']], on='id', how='left')
            df_in['Кабинет'] = profile
            df_cand['Кабинет'] = profile
            combined_df = pd.concat([df_cand, df_in]).sort_values(by='id').reset_index(drop=True)
            
            new_order = [15, 7, 8, 9, 10, 0, 12, 13, 11, 1, 2, 3, 4, 5, 6, 14]
            combined_df = combined_df.iloc[:, new_order]

            list_id = combined_df['id'].unique()
            df_goods_ex = df_goods[~df_goods['id'].isin(list_id)].copy()
            df_goods_ex['В_акции'] = 'список_товаров'
            df_goods_ex['price'] = df_goods_ex['price'].fillna(0)
            df_goods_ex['price'] = pd.to_numeric(df_goods_ex['price'], errors='coerce')
            df_goods_ex['price'] = df_goods_ex['price'].astype(int)
            df_goods_ex['Кабинет'] = profile
            combined_full_df = pd.concat([combined_df, df_goods_ex[['id', 'offer_id', 'name', 'stocks_present',  'price','В_акции', 'Кабинет']]])

            df_list.append(combined_full_df)
        except Exception as e:
            return f'error transforming actions df {e}'

    df_final = pd.concat(df_list).reset_index(drop=True)
    return df_final
