import datetime
import re
from typing import Optional

import pandas as pd
import aiohttp

from urllib.parse import urljoin

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config import URL
from bs4 import BeautifulSoup
from models.database import DATABASE_URL
from models.spimex_trading_results import SpimexTradingResult

Base = declarative_base()


async def fetch(session, url):
    async with session.get(url) as response:
        response.raise_for_status()  # Проверка на ошибки HTTP
        return await response.text()


async def download_file(session, file_link, trade_date):
    async with session.get(file_link) as file_response:
        file_response.raise_for_status()
        with open(f'data/oil_bulletin{trade_date}.xls', 'wb') as f:
            f.write(await file_response.read())
        print(f"Файл oil_bulletin{trade_date}.xls успешно скачан.")


async def parsing_trading_on_file() -> datetime.date:
    async with aiohttp.ClientSession() as session:
        html_content = await fetch(session, URL)
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

                await download_file(session, file_link, trade_date)
                return trade_date
            else:
                print("Не удалось извлечь дату из имени файла.")
        else:
            print("Не удалось найти ссылку на файл.")
    return None


async def get_data(trade_date: datetime.date) -> Optional[pd.DataFrame]:
    """
    Извлекает данные из файла бюллетеня по итогам торгов в Секции «Нефтепродукты»
    для заданной даты. Возвращает DataFrame с обработанными данными.
    """
    try:
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
        'volume': pd.to_numeric(filtered_data['Объем\nДоговоров\nв единицах\nизмерения']),
        'total': pd.to_numeric(filtered_data['Обьем\nДоговоров,\nруб.']),
        'count': pd.to_numeric(filtered_data['Количество\nДоговоров,\nшт.']),
        'date': trade_date,
        'created_on': pd.to_datetime('now'),
        'updated_on': pd.to_datetime('now')
    })

    print('Данные готовы для сохранения в базу данных')
    return spimex_trading_results


async def save_data_to_db(spimex_trading_results: pd.DataFrame) -> None:
    """
    Сохраняет данные из DataFrame в базу данных.
    """
    engine = create_async_engine(DATABASE_URL, future=True, echo=True)
    async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        async with session.begin():
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

                await session.commit()  # Коммитим все изменения после добавления всех объектов
                print('Данные успешно сохранены в базу данных')
            except Exception as e:
                await session.rollback()  # Откат изменений в случае ошибки
                print(f"Ошибка при сохранении данных в базу данных: {e}")
