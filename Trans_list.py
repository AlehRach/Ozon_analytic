import requests as re
import pandas as pd
import time
from datetime import datetime

def get_translist(client_id, api_key, profile='Gr', d_from='2024-11-01', d_to='2024-11-15'):

    headers = {
    'Client-Id': client_id,
    'Api-Key': api_key
    }
    trans_list_url = 'https://api-seller.ozon.ru/v3/finance/transaction/list'
    page = 1
    trans_list = []

    while True:
        params = {'filter': {'date': {
        "from": f"{d_from}T00:00:00.000Z",
        "to": f"{d_to}T23:59:59.999Z"}},
        'page': page,
        'page_size': 1000}

        try:
            res = re.post(trans_list_url, headers=headers, json=params)
            res.raise_for_status()
            trans_data = res.json()
            page_count = trans_data['result']['page_count']
            trans_list.extend(trans_data['result']['operations'])

            if page >= page_count:
                break
            page+=1
        except Exception as e:
            return f'ошибка получения данных Трансакции {profile} на странице {page}'
        time.sleep(0.5)
    print(f'Успех парсинг данные Трансакции {profile}')
    all_data = []
    # Обработка каждого элемента из списка данных

    try:
        for data in trans_list:
            # Извлекаем данные из posting
            posting_data = data['posting']
            items_data = data['items']
            services_data = data['services']
            
            # Создаем основной датафрейм с основными данными
            df = pd.DataFrame([{
                'operation_id': data['operation_id'],
                'operation_type': data['operation_type'],
                'operation_date': data['operation_date'],
                'operation_type_name': data['operation_type_name'],
                'delivery_charge': data['delivery_charge'],
                'return_delivery_charge': data['return_delivery_charge'],
                'accruals_for_sale': data['accruals_for_sale'],
                'sale_commission': data['sale_commission'],
                'amount': data['amount'],
                'type': data['type'],
                'delivery_schema': posting_data['delivery_schema'],
                'order_date': posting_data['order_date'],
                'posting_number': posting_data['posting_number'],
                'warehouse_id': posting_data['warehouse_id']
            }])
            
            # Преобразуем данные о товарах
            df_items = pd.json_normalize(items_data)
            df_items['operation_id'] = data['operation_id']
            df_items = df_items.rename(columns={'name': 'item_name', 'sku': 'item_sku'})
            
            # Преобразуем данные о сервисах
            df_services = pd.json_normalize(services_data)
            df_services['operation_id'] = data['operation_id']
            
            # Объединяем все данные в один датафрейм
            df = pd.merge(df, df_items, on='operation_id', how='left')
            df = pd.merge(df, df_services, on='operation_id', how='left')
            
            # Добавляем текущий обработанный датафрейм в общий список
            all_data.append(df)

        # Объединяем все датафреймы из списка в один итоговый
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['item_sku'] = final_df.groupby('posting_number')['item_sku'].transform(lambda x: x.ffill().bfill())
        final_df = final_df.rename(columns={'item_sku': 'sku'})
        print(f'Успех таблицы Трансакции {profile}')
    except Exception as e:
        return f'ошибка формирования таблицы Трансакции {profile}'
    
    operations = ['Доставка и обработка возврата, отмены, невыкупа', 'Оплата эквайринга', 'Услуга продвижения Бонусы продавца', 'Доставка покупателю', 'Звёздные товары',
                'Получение возврата, отмены, невыкупа от покупателя', 'Доставка покупателю — отмена начисления', 'Подписка Premium Plus',
                'Утилизация товара: Повреждённые из-за упаковки', 'Продвижение бренда', 'Приобретение отзывов на платформе', 'Трафареты', 'Кросс-докинг',
                'Обработка товара в составе грузоместа на FBO', 'Утилизация товара: Повреждённые, были у покупателя', 'Брак по вине Ozon на складе',
                'Потеря по вине Ozon в логистике', 'Начисление по спору', 'Услуга по обработке опознанных излишков в составе ГМ',
                'Услуга по бронированию места и персонала для поставки с неполным составом в составе ГМ', 'Утилизация товара: Вы не забрали в срок',
                'Корректировки стоимости услуг', 'Продвижение в поиске', 'Утилизация товара: Прочее']
    servs = ['MarketplaceServiceItemRedistributionReturnsPVZ', 'MarketplaceServiceItemDirectFlowLogisticVDC', 'MarketplaceServiceItemReturnNotDelivToCustomer', 'MarketplaceServiceItemReturnFlowLogistic',
                   'MarketplaceServiceItemDirectFlowLogistic', 'MarketplaceRedistributionOfAcquiringOperation', 'MarketplaceServicePremiumCashbackIndividualPoints',
                   'MarketplaceServiceItemReturnAfterDelivToCustomer', 'MarketplaceServiceItemReturnPartGoodsCustomer', 'MarketplaceServiceItemDelivToCustomer',
                   'ItemAgentServiceStarsMembership', 'MarketplaceServiceItemDisposalDetailed', 'MarketplaceServiceBrandCommission']
    
    message_list = []
    operations_extr = set(final_df['operation_type_name'].dropna().unique()) - set(operations)
    operations_mis = set(operations) - set(final_df['operation_type_name'].dropna().unique())
    if operations_extr:
        operations_extr_log = f'{profile} появились операци в operation_type_name: ', operations_extr
        print(operations_extr_log)
        message_list.extend(operations_extr_log)
    if operations_mis:
        operations_mis_log = f'{profile} пропали операци из operation_type_name: ', operations_mis
        print(operations_mis_log)
        message_list.extend(operations_mis_log)

    servs_extr = set(final_df['name'].dropna().unique()) - set(servs)
    servs_mis = set(servs) - set(final_df['name'].dropna().unique())
    if servs_extr:
        servs_extr_log = f'{profile} появились операци в servs_name: ', servs_extr
        print(servs_extr_log)
        message_list.extend(servs_extr_log)
    if servs_mis:
        servs_mis_log = f'{profile} пропали операци из servs_name: ', servs_mis
        print(servs_mis_log)
        message_list.extend(servs_mis_log)
    
    return final_df, message_list
