import pandas as pd


def calculate_percentage_change(value_1, value_2):
    if value_2:
        return round((value_1 - value_2) / abs(value_2) * 100, 2)
    return round(value_1 * 100, 2)


# Сохранение данных из словаря в датафрейм
crypto_data = {
    'asset': ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'LTCUSDT', 'ADAUSDT'],
    'price:': [29000, 1800, 0.55, 90, 0.25],
    'marketCap': [540000000000, 220000000000, 25000000000, 6000000000, 8000000000]
}

df_crypto = pd.DataFrame(crypto_data)
df_crypto.to_csv('df.csv')


# Пример градации активов за 5 минут:
five_min_before_data = pd.read_csv(f'five_min_before_24hr.csv')  # данные 5 минут назад
current_data = pd.read_csv(f'current_24hr.csv')  # текущие данные

# pd.Series.to_set = lambda self: set(self)  # преобразую объект в множество, чтобы найти пересечения у symbols 5 минут назад и symbols текущих данных, это нужно чтобы не возникало ошибки IndexError (например, если за эти 5 минут произошел листинг нового актива)

symbols = current_data.symbol.tolist()

volume_growth = []
price_growth = []

for symbol in symbols:
    current_data_index = current_data.index[current_data.symbol == symbol].tolist()[0]
    five_min_before_data_index = five_min_before_data.index[five_min_before_data.symbol == symbol].tolist()[0]

    # Нахождение процента изменения объёма
    current_daily_volume = current_data.loc[current_data_index, 'quoteVolume']
    five_min_daily_volume = five_min_before_data.loc[five_min_before_data_index, 'quoteVolume']

    volume_change_percent = calculate_percentage_change(value_1=current_daily_volume, value_2=five_min_daily_volume)
    volume_growth.append(volume_change_percent)

    # Нахождение процента изменения цены
    current_price = current_data.loc[current_data_index, 'lastPrice']
    five_min_before_price = five_min_before_data.loc[five_min_before_data_index, 'lastPrice']

    price_change_percent = calculate_percentage_change(value_1=current_price, value_2=five_min_before_price)
    price_growth.append(price_change_percent)

# Сортировка по объёму
current_data['local_volume_change_percent'] = volume_growth
current_data = current_data.sort_values('local_volume_change_percent', ascending=False).copy()
daily_volume_data = current_data[['symbol', 'local_volume_change_percent']]  # тот файл, что отправляется пользователю

# Сортировка по цене
current_data['local_price_change_percent'] = price_growth
current_data = current_data.sort_values('local_price_change_percent', ascending=False).copy()
price_data = current_data[['symbol', 'local_price_change_percent']]
