import pandas as pd

data = pd.read_csv(f'data.csv')
funding_rates = data['fundingRate'].astype(float).tolist()

positive_funding_rate = [rate for rate in funding_rates if rate > 0.01]  # положительные считаются только от 0.01%
positive_funding_rate_quantity = len(positive_funding_rate)

negative_funding_rate = [rate for rate in funding_rates if 0.005 != rate < 0.01]  # отрицательные должны быть меньше 0.01%, но не равны 0.005%
negative_funding_rate_quantity = len(negative_funding_rate)

neutral_funding_rate = [rate for rate in funding_rates if rate == 0.01 or rate == 0.05]  # нейтральные равны либо 0.01%, либо 0.005%
neutral_funding_rate_quantity = len(neutral_funding_rate)

print(f'Количество положительных фандингов: {positive_funding_rate_quantity}')
print(f'Количество отрицательных фандингов: {negative_funding_rate_quantity}')
print(f'Количество нейтральных фандингов: {neutral_funding_rate_quantity}')

