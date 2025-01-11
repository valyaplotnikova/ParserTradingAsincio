import datetime
import re
from typing import Optional

import pandas as pd
import requests

from urllib.parse import urljoin

from sqlalchemy.orm import sessionmaker

from config import URL
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from models.database import DATABASE_URL
from models.spimex_trading_results import SpimexTradingResult


def parsing_trading_on_file() -> datetime.date:
    """
    Извлекает ссылку на файл бюллетеня по итогам торгов в Секции «Нефтепродукты»,
    загружает файл и сохраняет его на диск. Возвращает дату торгов, извлеченную из имени файла.

    :return: Дата торгов в формате datetime.date.
    :raises ValueError: Если не удается извлечь дату из имени файла.
    :raises Exception: Если возникают ошибки при загрузке страницы или файла.
    """
    response = requests.get(URL)

    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Извлечение ссылки на файл
        link_tag = soup.find('a', class_='accordeon-inner__item-title link xls',
                             string='Бюллетень по итогам торгов в Секции «Нефтепродукты»')
        if link_tag:
            file_link = link_tag['href']
            if not file_link.startswith('http'):
                file_link = urljoin(URL, file_link)

            match = re.search(r'_(\d{14})\.xls', file_link)
            if match:
                date_str = match.group(1)  # Получаем строку даты
                # Преобразуем строку в объект datetime
                trade_date = datetime.datetime.strptime(date_str, '%Y%m%d%H%M%S').date()
                print(f'Дата торгов: {trade_date}')

            file_response = requests.get(file_link)
            if file_response.status_code == 200:
                with open(f'data/oil_bulletin{trade_date}.xls', 'wb') as f:
                    f.write(file_response.content)
                print(f"Файл oil_bulletin{trade_date}.xls успешно скачан.")
            else:
                print(f"Ошибка при загрузке файла: {file_response.status_code}")
        else:
            print("Не удалось найти ссылку на файл.")
    else:
        print(f"Ошибка при загрузке страницы: {response.status_code}")
    return trade_date


def get_data(trade_date: datetime.date) -> Optional[pd.DataFrame]:
    """
    Извлекает данные из файла бюллетеня по итогам торгов в Секции «Нефтепродукты»
    для заданной даты. Возвращает DataFrame с обработанными данными.

    :param trade_date: Дата торгов в формате datetime.date.
    :return: DataFrame с данными или None, если данные не найдены.
    :raises FileNotFoundError: Если файл не найден.
    :raises ValueError: Если не удается найти строку с нужной информацией.
    """
    try:
        # Сначала загружаем весь файл, чтобы найти стартовую строку
        temp_df = pd.read_excel(f'data/oil_bulletin{trade_date}.xls', header=None)
    except FileNotFoundError:
        print(f"Файл data/oil_bulletin{trade_date}.xls не найден.")
        return None

    # Поиск строки, где начинается нужная информация
    row_start = None
    for row in range(temp_df.shape[0]):
        for col in range(temp_df.shape[1]):
            if temp_df.iat[row, col] == 'Единица измерения: Метрическая тонна':
                row_start = row
                break
        if row_start is not None:
            break

    if row_start is None:
        raise ValueError("Не удалось найти строку с 'Единица измерения: Метрическая тонна'")

    # Загружаем данные, начиная с строки после стартовой строки
    header_row = row_start + 1
    df = pd.read_excel(f'data/oil_bulletin{trade_date}.xls', header=header_row)

    # Преобразование типов
    df['Количество\nДоговоров,\nшт.'] = pd.to_numeric(df['Количество\nДоговоров,\nшт.'], errors='coerce')

    filtered_data = df[
        (df['Количество\nДоговоров,\nшт.'] > 0) &
        (df['Наименование\nИнструмента'].notna())
    ]

    if filtered_data.empty:
        print("Нет данных для сохранения в базу данных.")
        return None

    # Создание нового DataFrame с нужной структурой
    spimex_trading_results = pd.DataFrame({
        'exchange_product_id': filtered_data['Код\nИнструмента'],
        'exchange_product_name': filtered_data['Наименование\nИнструмента'],
        'oil_id': filtered_data['Код\nИнструмента'].str[:4],
        'delivery_basis_id': filtered_data['Код\nИнструмента'].str[4:7],
        'delivery_basis_name': filtered_data['Базис\nпоставки'],
        'delivery_type_id': filtered_data['Код\nИнструмента'].str[-1],
        'volume': filtered_data['Объем\nДоговоров\nв единицах\nизмерения'],
        'total': filtered_data['Обьем\nДоговоров,\nруб.'],
        'count': filtered_data['Количество\nДоговоров,\nшт.'],
        'date': trade_date,
        'created_on': pd.to_datetime('now'),
        'updated_on': pd.to_datetime('now')
    })

    print('Данные готовы для сохранения в базу данных')
    return spimex_trading_results


def save_data_to_db(spimex_trading_results: pd.DataFrame) -> None:
    """
    Сохраняет данные из DataFrame в базу данных.

    :param spimex_trading_results: DataFrame с данными для сохранения.
    :raises Exception: Если возникает ошибка при сохранении данных в базу данных.
    """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for index, row in spimex_trading_results.iterrows():
            result = SpimexTradingResult(
                exchange_product_id=row['exchange_product_id'],
                exchange_product_name=row['exchange_product_name'],
                oil_id=row['oil_id'],
                delivery_basis_id=row['delivery_basis_id'],
                delivery_basis_name=row['delivery_basis_name'],
                delivery_type_id=row['delivery_type_id'],
                volume=row['volume'],
                total=row['total'],
                count=row['count'],
                date=row['date'],
                created_on=row['created_on'],
                updated_on=row['updated_on']
            )
            session.add(result)

        session.commit()  # Коммитим все изменения после добавления всех объектов
        print('Данные успешно сохранены в базу данных')
    except Exception as e:
        session.rollback()  # Откат изменений в случае ошибки
        print(f"Ошибка при сохранении данных в базу данных: {e}")
    finally:
        session.close()
