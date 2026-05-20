from coinx.repositories.homepage_series import (
    format_number,
    get_homepage_series_data,
)


def get_coin_data(symbol='BTCUSDT'):
    coins = get_homepage_series_data(symbols=[symbol])
    if not coins:
        return {
            'symbol': symbol,
            'current_open_interest': None,
            'current_open_interest_formatted': 'N/A',
            'current_open_interest_value': None,
            'current_open_interest_value_formatted': 'N/A',
            'current_price': None,
            'current_price_formatted': 'N/A',
            'price_change': None,
            'price_change_percent': None,
            'price_change_formatted': 'N/A',
            'net_inflow': {},
            'changes': {},
        }
    return coins[0]


def get_all_coins_data(symbols=None):
    return get_homepage_series_data(symbols=symbols)

