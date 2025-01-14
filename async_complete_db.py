import re
import datetime
import aiohttp
import asyncio

from urllib.parse import urljoin
from config import URL
from bs4 import BeautifulSoup

from models.database import create_db, drop_db
from parser_async import get_data, save_data_to_db, fetch, download_file


async def get_trading_all_dates_and_files(queue: asyncio.Queue) -> None:
    """
    Извлекает все даты торгов и соответствующие ссылки на файлы с сайта,
    добавляя их в асинхронную очередь.

    :param queue: Асинхронная очередь для хранения ссылок на файлы.
    """
    page_number = 1

    async with aiohttp.ClientSession() as session:
        while True:
            response = await fetch(session, f"{URL}?page=page-{page_number}")

            if response:
                soup = BeautifulSoup(response, 'html.parser')

                link_tags = soup.find_all('a', class_='accordeon-inner__item-title link xls')
                if not link_tags:
                    print(f"На странице {page_number} нет ссылок на файлы.")
                    break

                for link_tag in link_tags:
                    file_link = link_tag['href']
                    if not file_link.startswith('http'):
                        file_link = urljoin(URL, file_link)
                    match = re.search(r'_(\d{14})\.xls', file_link)
                    if match:
                        date_str = match.group(1)
                        trade_date = datetime.datetime.strptime(date_str, '%Y%m%d%H%M%S').date()
                        if trade_date >= datetime.datetime(2023, 1, 1).date():
                            await queue.put((trade_date, file_link))  # Добавляем в очередь
                        else:
                            break

                # Проверка на наличие следующей страницы
                next_page = soup.select_one('.bx-pag-next a')
                if next_page:
                    next_page_url = next_page['href']
                    match = re.search(r'page=page-(\d+)', next_page_url)
                    if match:
                        page_number = int(match.group(1))  # Переходим к следующей странице
                        print(f"Переход на страницу {page_number}...")
                    else:
                        print("Не удалось извлечь номер следующей страницы.")
                        break
                else:
                    print("Следующая страница не найдена.")
                    break
            else:
                break


async def process_files(queue: asyncio.Queue) -> None:
    """
    Асинхронно обрабатывает скачивание файлов и сохранение данных в БД.

    :param queue: Асинхронная очередь для получения ссылок на файлы и дат торговли.
    """
    async with aiohttp.ClientSession() as session:
        while True:
            trade_date, link = await queue.get()  # Получаем ссылку из очереди
            if link is None:  # Если получена сигнальная метка завершения
                break
            await download_file(session, trade_date, link)
            spimex_trading_results = await get_data(trade_date)  # Получаем данные асинхронно
            await save_data_to_db(spimex_trading_results)  # Сохраняем данные в БД


async def main():
    """
    Основная функция, которая запускает асинхронные задачи для извлечения ссылок на файлы
    и загрузки данных в базу данных.
    """
    await drop_db()
    await create_db()
    queue = asyncio.Queue()

    # Запускаем задачи для загрузки файлов
    processing_task = asyncio.create_task(process_files(queue))

    # Запускаем задачу для извлечения ссылок на файлы
    await get_trading_all_dates_and_files(queue)

    # Завершаем задачу обработки
    await queue.put((None, None))  # Отправляем сигнальную метку завершения
    await processing_task  # Ждем завершения обработки

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    asyncio.run(main())
    end_time = datetime.datetime.now()
    print(f'Программа отработала за {end_time - start_time}')
