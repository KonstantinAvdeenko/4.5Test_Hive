import pandas as pd
from datetime import datetime as dt


PATH_RAW = './csv_files/raw/'
PATH_PROC = './csv_files/processed/'
CNT_PARTS = 10


def range_data(df):
    """ранжирование данных в dataframe"""

    size_part = len(df) // CNT_PARTS
    tail = len(df) % CNT_PARTS
    new_col_data = []
    for i in range(1, CNT_PARTS + 1):
        if tail == 0:
            new_col_data.extend([i] * size_part)
        else:
            # Если фрейм не делиться на равные части,
            # то остаток от деления распределим равномерно
            new_col_data.extend([i] * (size_part + 1))
            tail -= 1
    df['Range'] = new_col_data
    return df


def extract_year(df):
    """выносит год подписки в отдельный столбец"""

    year_data = []
    for date in df['Subscription Date']:
        year_data.append(dt.strptime(date, '%Y-%m-%d').year)
    df['Subscription Year'] = year_data
    return df


if __name__ == '__main__':
    df = pd.read_csv(PATH_RAW + 'customers.csv')
    extract_year(range_data(df)).to_csv(
        PATH_PROC + 'customers.csv', sep=';', index=False
    )
    df = pd.read_csv(PATH_RAW + 'organizations.csv')
    range_data(df).to_csv(
        PATH_PROC + 'organizations.csv', sep=';', index=False
    )
    df = pd.read_csv(PATH_RAW + 'people.csv')
    range_data(df).to_csv(PATH_PROC + 'people.csv', sep=';', index=False)