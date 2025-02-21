import pandas as pd
import numpy as np
import time
from functools import reduce

def get_tabletrans(trans_list_df, goods, profile):
    try:

        # filtr
        df_sell = trans_list_df[(trans_list_df['operation_type_name'] == 'Доставка покупателю') & (trans_list_df['name'].isin(['MarketplaceServiceItemDirectFlowLogistic', np.nan]))].copy()
        df_sell['Выкуплено_шт'] = 1

        df_return = trans_list_df[trans_list_df['operation_type_name'].isin(['Получение возврата, отмены, невыкупа от покупателя', 'Доставка покупателю — отмена начисления'])].copy()
        df_return['Возвраты'] = 1

        df_declnonredempt = trans_list_df[trans_list_df['name'].isin(['MarketplaceServiceItemReturnNotDelivToCustomer', 'MarketplaceServiceItemReturnPartGoodsCustomer'])].copy()
        df_declnonredempt['Отмены_невыкупы'] = 1

        # drop_duplicates
        #df_sell_grpost = df_sell[['posting_number', 'sku', 'Выкуплено_шт', 'accruals_for_sale', 'sale_commission']].groupby('posting_number', group_keys=False).apply(lambda x: x.drop_duplicates()).reset_index(drop=True)
        #df_sell_grpost = df_sell_grpost.rename(columns={'accruals_for_sale': 'Сумма_заказы', 'sale_commission': 'Комиссия'})
        df_sell_grpost = df_sell.rename(columns={'accruals_for_sale': 'Сумма_заказы', 'sale_commission': 'Комиссия'})

        df_return_grpost = df_return[['posting_number', 'sku', 'Возвраты', 'accruals_for_sale', 'sale_commission']].groupby('posting_number', group_keys=False).apply(lambda x: x.drop_duplicates()).reset_index(drop=True)
        df_return_grpost = df_return_grpost.rename(columns={'accruals_for_sale': 'Получение_возврата', 'sale_commission': 'sale_commission_r'})

        # groupby
        goods_msell = goods.merge(df_sell_grpost, on='sku', how='outer')
        goods_msell['Articul'] = goods_msell['Articul'].fillna('неопределен')
        goods_msell_gr = goods_msell.groupby('Articul').agg({
            'Выкуплено_шт': 'sum',
            'Сумма_заказы': 'sum',
            'Комиссия': 'sum'
        }).reset_index()

        df_return_grpost_filt = df_return_grpost[df_return_grpost['Получение_возврата'] != 0]
        goods_return = goods.merge(df_return_grpost_filt, on='sku', how='outer')
        goods_return['Articul'] = goods_return['Articul'].fillna('неопределен')
        goods_return_gr = goods_return.groupby('Articul').agg({
            'Возвраты': 'sum',
            'Получение_возврата': 'sum',
            'sale_commission_r': 'sum'
        }).reset_index()

        goods_declnonredempt = goods.merge(df_declnonredempt[['sku', 'Отмены_невыкупы']], on='sku', how='outer')
        goods_declnonredempt['Articul'] = goods_declnonredempt['Articul'].fillna('неопределен')
        goods_declnonredempt_gr = goods_declnonredempt.groupby('Articul')['Отмены_невыкупы'].sum().reset_index()

        goods_msell_gr = goods_msell_gr.merge(goods_return_gr, on='Articul', how='outer')
        goods_msell_gr = goods_msell_gr.merge(goods_declnonredempt_gr, on='Articul', how='outer')

        def process_data(df, operation_type_name, name, new_column_name, oper_name, goods, dupls='neg'):
            filtered_df = df[df['operation_type_name'] == operation_type_name].copy()
            if isinstance(name, list) and name:
                filtered_df = filtered_df[filtered_df['name'].isin(name)]
            elif name:
                filtered_df = filtered_df[filtered_df['name'] == name]
            filtered_df = filtered_df.rename(columns={oper_name: new_column_name})
            if dupls == 'pos':
                filtered_df = filtered_df.drop_duplicates(subset=['operation_id'])
            filtered_merg = goods[['sku', 'Articul']].merge(filtered_df[['sku', new_column_name]], on='sku', how='outer')
            filtered_merg['Articul'] = filtered_merg['Articul'].fillna('неопределен')
            grouped_df = filtered_merg.groupby('Articul')[new_column_name].sum().reset_index()
            return grouped_df

        # Обработка данных с помощью функции
        df_logist = process_data(trans_list_df, 'Доставка покупателю', 'MarketplaceServiceItemDirectFlowLogistic', 'Прямая_логистика', 'price', goods)
        df_lastmile = process_data(trans_list_df, 'Доставка покупателю', 'MarketplaceServiceItemDelivToCustomer', 'Последняя_миля', 'price', goods)
        df_return = process_data(trans_list_df, 'Доставка и обработка возврата, отмены, невыкупа', 'MarketplaceServiceItemReturnAfterDelivToCustomer', 'обработка_возврата', 'amount', goods)
        df_declines = process_data(trans_list_df, 'Доставка и обработка возврата, отмены, невыкупа', 'MarketplaceServiceItemReturnNotDelivToCustomer', 'отмен', 'amount', goods)
        df_nonredempt = process_data(trans_list_df, 'Доставка и обработка возврата, отмены, невыкупа', 'MarketplaceServiceItemReturnPartGoodsCustomer', 'невыкуп', 'amount', goods)
        df_logdeclines = process_data(trans_list_df, 'Доставка и обработка возврата, отмены, невыкупа', ['MarketplaceServiceItemDirectFlowLogistic', 'MarketplaceServiceItemDirectFlowLogisticVDC'], 'логистика_отмен', 'price', goods)
        df_delivrdecliness = process_data(trans_list_df, 'Доставка покупателю — отмена начисления', '', 'Доставка_отмена_начисления', 'price', goods)
        df_argue = process_data(trans_list_df, 'Начисление по спору', '', 'Начисление_спор', 'amount', goods)
        df_ozonlostlog = process_data(trans_list_df, 'Потеря по вине Ozon в логистике', '', 'Потеря_Ozon_логист', 'amount', goods)
        df_ozonloststock = process_data(trans_list_df, 'Брак по вине Ozon на складе', '', 'Потеря_Ozon_склад', 'amount', goods)
        df_eqviring = process_data(trans_list_df, 'Оплата эквайринга', '', 'эквайринг', 'amount', goods, 'pos')
        df_corrpriseserv = process_data(trans_list_df, 'Корректировки стоимости услуг', '', 'Корр_стоимост_услуг', 'amount', goods)
        df_trafaret = process_data(trans_list_df, 'Трафареты', '', 'Трафареты', 'amount', goods)
        df_promsearch = process_data(trans_list_df, 'Продвижение в поиске', '', 'Прод_поиск', 'amount', goods)
        df_stargoods = process_data(trans_list_df, 'Звёздные товары', '', 'Звёздн_товары', 'amount', goods)
        df_getreviews = process_data(trans_list_df, 'Приобретение отзывов на платформе', '', 'Приобретен_отзывов', 'amount', goods)
        df_prombrand = process_data(trans_list_df, 'Продвижение бренда', '', 'Прод_бренда', 'amount', goods)
        df_premplus = process_data(trans_list_df, 'Подписка Premium Plus', '', 'Premium_Plus', 'amount', goods)
        df_prombonus = process_data(trans_list_df, 'Услуга продвижения Бонусы продавца', '', 'продвиж_бонус', 'amount', goods, 'pos')
        df_crossdock = process_data(trans_list_df, 'Кросс-докинг', '', 'Кросс_докинг', 'amount', goods)
        df_cargofbo = process_data(trans_list_df, 'Обработка товара в составе грузоместа на FBO', '', 'Обработка_грузоместа_FBO', 'amount', goods)
        df_bookingplacestuff = process_data(trans_list_df, 'Услуга по бронированию места и персонала для поставки с неполным составом в составе ГМ', '', 'Бронь_места_персонал_поставки_составеГМ', 'amount', goods)
        df_identsurlpus = process_data(trans_list_df, 'Услуга по обработке опознанных излишков в составе ГМ', '', 'Опознанн_излишков_составеГМ', 'amount', goods)
        df_disposalontime = process_data(trans_list_df, 'Утилизация товара: Вы не забрали в срок', '', 'Утилизация_не_всрок', 'amount', goods)
        df_disposalpackage = process_data(trans_list_df, 'Утилизация товара: Повреждённые из-за упаковки', '', 'Утилизация_Повреждённые_упаковки', 'amount', goods)
        df_disposalaftercustom = process_data(trans_list_df, 'Утилизация товара: Повреждённые, были у покупателя', '', 'Утилизация_Повреждённые_покупателя', 'amount', goods)
        df_disposalothers = process_data(trans_list_df, 'Утилизация товара: Прочее', '', 'Утилизация_Прочее', 'amount', goods)
        df_pinreview = process_data(trans_list_df, 'Закрепление отзыва', '', 'Закреп_отзыва', 'amount', goods)
        df_defectgoods = process_data(trans_list_df, 'Обработка брака с приемки', '', 'Брака_приемки', 'amount', goods)

        # Список для объединения данных
        logist_list_gr = [df_logist, df_lastmile, df_return, df_declines, df_nonredempt, df_logdeclines, df_delivrdecliness, df_argue, df_ozonlostlog, df_ozonloststock, df_eqviring,
                        df_corrpriseserv, df_trafaret, df_promsearch, df_stargoods, df_getreviews, df_prombrand, df_premplus, df_prombonus, df_crossdock, df_cargofbo, df_bookingplacestuff,
                        df_identsurlpus, df_disposalontime, df_disposalpackage, df_disposalaftercustom, df_disposalothers, df_pinreview, df_defectgoods]

        goods_gr = reduce(lambda left, right: left.merge(right, on='Articul', how='outer'), logist_list_gr)
        goods_msell_gr = goods_msell_gr.merge(goods_gr, on='Articul', how='outer')
        print(f'таблица начесления метка before stocks {profile} success')

        goods_stock = goods.groupby('Articul').agg({
            'sku': 'first',
            'id': 'first',
            'stocks_present': 'sum',
            'stocks_reserved': 'sum'
        }).reset_index()
        goods_msell_fin = goods_stock.merge(goods_msell_gr, on='Articul', how='outer')

        goods_msell_fin['Кабинет'] = profile
        goods_msell_fin.insert(1, 'Кабинет', goods_msell_fin.pop('Кабинет'))
    except Exception as e:
        return f'ошибка формирования финальной таблицы Начисления {profile}'
    return goods_msell_fin