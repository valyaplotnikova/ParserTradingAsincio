import re
import datetime
from typing import List, Tuple

import requests

from urllib.parse import urljoin
from config import URL
from bs4 import BeautifulSoup

from parser import get_data, save_data_to_db


def get_trading_all_dates_and_files() -> List[Tuple[datetime.date, str]]:
    """
    Извлекает все даты торгов и соответствующие ссылки на файлы с сайта.

    :return: Список кортежей, содержащих дату торгов и ссылку на файл.
    :raises Exception: Если возникает ошибка при загрузке страницы.
    """
    page_number = 1
    all_files = []

    while True:
        response = requests.get(f"{URL}?page=page-{page_number}")

        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

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
                        all_files.append((trade_date, file_link))
                    else:
                        break

            # Проверка на наличие следующей страницы
            next_page = soup.select_one('.bx-pag-next a')
            if next_page:
                next_page_url = next_page['href']
                # Извлекаем номер страницы из URL
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
            print(f"Ошибка при загрузке страницы: {response.status_code}")
            break

    return all_files


def download_files(trade_date: datetime.date, file_link: str) -> None:
    """
    Скачивает файл по указанной ссылке и сохраняет его на диск.

    :param trade_date: Дата торгов, используемая для формирования имени файла.
    :param file_link: Ссылка на файл для скачивания.
    :raises Exception: Если возникает ошибка при загрузке файла.
    """
    file_response = requests.get(file_link)
    if file_response.status_code == 200:
        file_name = f'data/oil_bulletin{trade_date}.xls'
        with open(file_name, 'wb') as f:
            f.write(file_response.content)
        print(f"Файл {file_name} успешно скачан.")
    else:
        print(f"Ошибка при загрузке файла: {file_response.status_code}")


if __name__ == "__main__":

    start_time = datetime.datetime.now()
    all_files = get_trading_all_dates_and_files()
    for trade_date, link in all_files:
        download_files(trade_date, link)
        spimex_trading_results = get_data(trade_date)
        save_data_to_db(spimex_trading_results)
    end_time = datetime.datetime.now()
    print(f'Программа отработала за {end_time-start_time}')
