import asyncio

from models.database import create_db, drop_db
from parser_async import parsing_trading_on_file, get_data, save_data_to_db


async def main():
    await drop_db()
    await create_db()
    trade_date = await parsing_trading_on_file()
    if trade_date:
        spimex_trading_results = await get_data(trade_date)
        if spimex_trading_results is not None:
            await save_data_to_db(spimex_trading_results)


if __name__ == '__main__':
    asyncio.run(main())
