from models.database import create_db, drop_db
from parser import parsing_trading_on_file, get_data, save_data_to_db


def main():
    drop_db()
    create_db()
    trade_date = parsing_trading_on_file()
    spimex_trading_results = get_data(trade_date)
    save_data_to_db(spimex_trading_results)


if __name__ == '__main__':
    main()
