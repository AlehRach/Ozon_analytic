import requests as re
import time
import pandas as pd

def get_all_goods(client_id, api_key, profile='Gr'):

    # URL для получения списка товаров
    list_url = 'https://api-seller.ozon.ru/v3/product/list'
    # URL для получения информации о конкретном товаре
    info_url = 'https://api-seller.ozon.ru/v3/product/info/list'

    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key
    }

    # Функция для получения списка всех товаров
    def get_all_products():

        all_products = []
        last_id = ''

        while True:
            params = {
                "filter": {"visibility": "ALL"},
                "last_id": last_id,
                "limit": 1000
            }
            try:
                response = re.post(list_url, headers=headers, json=params)
                response.raise_for_status()
                data = response.json()

            except Exception as e:
                return f"Ошибка получения списка товаров {profile}: {response.status_code}, {response.text}"
            products = data.get("result", {}).get("items", [])
            if not products:
                break
            all_products.extend(products)
            if len(data['result']['items']) >= 1000:
                last_id = data.get("result", {}).get("last_id")

            else:
                break

            # Задержка для избежания превышения лимитов API
            time.sleep(0.5)

        return all_products


    goods = get_all_products()
    if isinstance(goods, str):
        return (goods)
    prod_list = []
    for product in goods:
        product_id = product.get("product_id")
        prod_list.append(product_id)

    goods_batch_list = []
    for i in range(0, len(prod_list), 1000):
        batch = prod_list[i:i + 1000]

        params = {
            'product_id': batch
        }

        try:
            res = re.post(info_url, headers=headers, json=params)
            res.raise_for_status()
            goods_batch = res.json()
            goods_batch_list.append(goods_batch)
        except Exception as e:
            return f"Ошибка при загрузке инфо товаров {profile}: {e}"
    print(f"Данные goods успешно загружены для профиля {profile}")
    try:
        items = goods_batch_list[0]['items']
        rows = []

        for item in items:
            row = {
                'id': item['id'],
                'offer_id': item['offer_id'],
                'name': item['name'],
                'marketing_price': item['marketing_price'],
                'old_price': item['old_price'],
                'price': item['price'],
                'is_super': item['is_super'],
                'stocks_present': item['stocks']['stocks'][0]['present'] if item['stocks']['stocks'] else 0,
                'stocks_reserved': item['stocks']['stocks'][0]['reserved'] if item['stocks']['stocks'] else 0,
                'sku': item['stocks']['stocks'][0]['sku'] if item['stocks']['stocks'] else 0,
            }
            rows.append(row)
        final_df = pd.DataFrame(rows)
        print('final_df success')
    except Exception as e:
        return f"Ошибка при обработке данных и сохранении CSV для профиля {profile}: {e}"
    return final_df
