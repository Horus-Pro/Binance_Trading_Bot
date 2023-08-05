import time
import logging
from binance.client import Client
import pprint
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import mysql.connector
import requests
import configparser

logging.basicConfig(level=logging.DEBUG, filename='Logs/Trading_Bot/logs.txt', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#My API
api_key = "Put you key here"
api_secret = "Put your secret here"


bot_key = 'Put Telegram bot Key Here to receive Notification'
telegram_user = "Put you telegram user number here"

def dateandtime():
    date_time = datetime.now() + timedelta(hours=0)
    updated_time = date_time.strftime("%Y/%m/%d - %H:%M:%S")
    return str(updated_time)

def send_update(chat_id, msg):
    url = f"https://api.telegram.org/bot{bot_key}/sendMessage?chat_id={chat_id}&text={msg}"
    requests.get(url)

def get_data_frame(symbol):
    starttime = '1 day ago UTC'
    interval = '5m'
    bars = client.get_historical_klines(symbol, interval, starttime)
    #pprint.pprint(bars)
    for line in bars:
        del line[5:]
    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close'])
    return df

def plot_graph(df):
    df = df.astype(float)
    df[['close', 'sma', 'upper', 'lower']].plot()
    plt.xlabel('Date', fontsize=18)
    plt.ylabel('Close price', fontsize=18)
    x_axis = df.index
    plt.fill_between(x_axis, df['lower'], df['upper'], color='grey', alpha=0.30)
    plt.scatter(df.index, df['buy'], color='green', label='Buy', marker='^', alpha= 1)
    plt.scatter(df.index, df['sell'], color='red', label='Sell', marker='v', alpha= 1)
    plt.show()

def bollinger_trade_logic(balance, symbol, quantity):
    symbol_df = get_data_frame(symbol)
    period = 20
    symbol_df['sma'] = symbol_df['close'].rolling(period).mean()
    symbol_df['std'] = symbol_df['close'].rolling(period).std()
    symbol_df['upper'] = symbol_df['sma'] + (2 * symbol_df['std'])
    symbol_df['lower'] = symbol_df['sma'] - (2 * symbol_df['std'])
    symbol_df.set_index('date', inplace=True)
    symbol_df.index = pd.to_datetime(symbol_df.index, unit='ms')

    close_list = pd.to_numeric(symbol_df['close'], downcast='float')
    upper_list = pd.to_numeric(symbol_df['upper'], downcast='float')
    lower_list = pd.to_numeric(symbol_df['lower'], downcast='float')

    symbol_df['buy'] = np.where(close_list < lower_list, symbol_df['close'], np.NAN)
    symbol_df['sell'] = np.where(close_list > upper_list, symbol_df['close'], np.NAN)
    with open('output.txt', 'w') as f:
        f.write(symbol_df.to_string())
    #plot_graph(symbol_df)
    buy_or_sell(balance, symbol_df, symbol, quantity)

def buy_or_sell(balance, df, symbol, quantity):
    buy = pd.to_numeric(df['buy'], downcast='float')
    sell = pd.to_numeric(df['sell'], downcast='float')
    current_price = client.get_symbol_ticker(symbol = symbol)
    last_sell_time = datetime.strptime(str(sell.last_valid_index()), "%Y-%m-%d %H:%M:%S")
    # current_price = {'symbol': 'ETHUSDT', 'price': 2000}
    commission = 0.9999
    # print(current_price)
    print(f"{dateandtime()} ***** Current {symbol} Price: {current_price['price']} *****")
    current_balance = float(client.get_account()['balances'][11]['free'])

    # Buy Order
    logging.info(f"{symbol} Buy price set as {buy.dropna().iloc[-1]}")
    print(f"{dateandtime()} {symbol} Buy price set as {buy.dropna().iloc[-1]}")
    mydb = mysql.connector.connect(
        host="160.153.131.153",
        user="SamuraiCrypto",
        password="#LvLC.$*ca0b",
        database="CryptoVerse_db"
    )
    mycursor = mydb.cursor()
    sql = "SELECT * FROM trading_bot WHERE symbol = %s AND status = %s"
    val = (symbol, 1)
    mycursor.execute(sql, val)
    buy_orders = mycursor.fetchall()
    max_orders = 5
    config = configparser.ConfigParser()
    config.read('Binance_Trading.ini')
    status = int(config["DEFAULT"]['status'])
    if len(buy_orders) < 1:
        if float(current_price['price']) <= buy.dropna().iloc[-1]:
            if balance > (quantity * float(current_price['price'])) and status < 2:
                logging.info(f"Buy Buy Buy... {current_price['price']} <= {buy.dropna().iloc[-1]}")
                print(f"{dateandtime()} Buy Buy Buy... {current_price['price']} <= {buy.dropna().iloc[-1]}")
                # buy_order = client.order_market_buy(symbol=symbol, quantity=quantity)
                # print(buy_order)
                sql = "INSERT INTO trading_bot (symbol , buy_price, quantity, status) VALUES (%s, %s, %s, %s)"
                val = (symbol , current_price['price'] , (quantity*commission), 1)
                mycursor.execute(sql, val)
                mydb.commit()
                response = f"New buy order... {symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"☑️ {response}")
            else:
                if status >= 2:
                    response = f"STOP LOSS limit reached {status} "
                elif balance < (quantity * float(current_price['price'])):
                    response = "No enough balance for order "
                else:
                    response = "Error "
                response += f"{symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"⚠️ {response}")
    elif len(buy_orders) >= 1 and len(buy_orders) < max_orders:
        sql = "SELECT * FROM trading_bot WHERE symbol = %s AND status = %s ORDER BY id DESC LIMIT %s"
        val = (symbol, 1, 1)
        mycursor.execute(sql, val)
        last_order = mycursor.fetchall()
        cooldown = last_order[0][6] + timedelta(hours=7, minutes=30)
        if float(current_price['price']) < (float(last_order[0][2]) * 0.99) and float(current_price['price']) <= buy.dropna().iloc[-1]:
            if balance > (quantity * float(current_price['price'])) and status < 2:
                logging.info(f"Buy Buy Buy... {current_price['price']} < {(float(last_order[0][2]) * 0.99)} (-1%) [orders > 2]")
                print(f"{dateandtime()} Buy Buy Buy... {current_price['price']} < {(float(last_order[0][2]) * 0.99)} (-1%) [orders > 2]")
                # buy_order = client.order_market_buy(symbol=symbol, quantity=quantity)
                # print(buy_order)
                sql = "INSERT INTO trading_bot (symbol , buy_price, quantity, status) VALUES (%s, %s, %s, %s)"
                val = (symbol, current_price['price'], (quantity*commission), 1)
                mycursor.execute(sql, val)
                mydb.commit()
                response = f"New buy order... {symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"☑️ {response}")
            else:
                if status >= 2:
                    response = f"STOP LOSS limit reached {status} "
                elif balance < (quantity * float(current_price['price'])):
                    response = "No enough balance for order "
                else:
                    response = "Error "
                response += f"{symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"⚠️ {response}")
        elif float(current_price['price']) <= buy.dropna().iloc[-1] and datetime.now() > cooldown:
            if balance > (quantity * float(current_price['price'])) and status < 2:
                print(f"{datetime.now()}    {cooldown}")
                logging.info(f"Buy Buy Buy... {current_price['price']} < {buy.dropna().iloc[-1]} and cooldown passed.")
                print(f"{dateandtime()} Buy Buy Buy... {current_price['price']} < {buy.dropna().iloc[-1]} and cooldown passed.")
                # buy_order = client.order_market_buy(symbol=symbol, quantity=quantity)
                # print(buy_order)
                sql = "INSERT INTO trading_bot (symbol , buy_price, quantity, status) VALUES (%s, %s, %s, %s)"
                val = (symbol, current_price['price'], (quantity*commission), 1)
                mycursor.execute(sql, val)
                mydb.commit()
                response = f"New buy order... {symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"☑️ {response}")
            else:
                if status >= 2:
                    response = f"STOP LOSS limit reached {status} "
                elif balance < (quantity * float(current_price['price'])):
                    response = "No enough balance for order "
                else:
                    response = "Error "
                response += f"{symbol}\n💳 Buy price: {current_price['price']}\n💎 Quantity: {quantity}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"⚠️ {response}")

    # Sell Order
    sql = "SELECT * FROM trading_bot WHERE symbol = %s AND status = %s"
    val = (symbol, 1)
    mycursor.execute(sql, val)
    myresult = mycursor.fetchall()
    logging.info(f"{symbol} Sell price set as {sell.dropna().iloc[-1]}")
    print(f"{dateandtime()} {symbol} Sell price set as {sell.dropna().iloc[-1]}")
    if len(myresult) > 0:
        for x in myresult:
            order_time = x[6] + timedelta(hours=7)
            # sell_quantity = float(x[4]) - (float(x[4])*0.005)
            if float(current_price['price']) >= sell.dropna().iloc[-1] and sell.dropna().iloc[-1] > float(x[2]) and last_sell_time > order_time:
                logging.info(f"Sell Sell Sell... {current_price['price']} >= {sell.dropna().iloc[-1]}")
                print(f"{dateandtime()} Sell Sell Sell... {current_price['price']} >= {sell.dropna().iloc[-1]}")
                # sell_order = client.order_market_sell(symbol=symbol, quantity=float(x[4]))
                # print(sell_order)
                sql = "UPDATE trading_bot SET sell_price = %s, status = %s WHERE id = %s"
                val = (current_price['price'], 0, x[0])
                mycursor.execute(sql, val)
                mydb.commit()
                response = f"Order [{x[0]}] {symbol} sold...\n💳 Buy price: {x[2]}\n💵 Sell price: {current_price['price']}\n💎 Quantity: {x[4]}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"✅ {response}")
            elif float(current_price['price']) < (float(x[2]) * 0.95):
                logging.info(f"Sell Sell Sell... {current_price['price']} >= {sell.dropna().iloc[-1]}")
                print(f"{dateandtime()} Sell Sell Sell... {current_price['price']} >= {sell.dropna().iloc[-1]}")
                # sell_order = client.order_market_sell(symbol=symbol, quantity=float(x[4]))
                # print(sell_order)
                sql = "UPDATE trading_bot SET sell_price = %s, status = %s WHERE id = %s"
                val = (current_price['price'], 0, x[0])
                mycursor.execute(sql, val)
                mydb.commit()
                response = f"[STOP LOSS] Order [{x[0]}] {symbol} sold...\n💳 Buy price: {x[2]}\n💵 Sell price: {current_price['price']}\n💎 Quantity: {x[4]}\n💰 Current Balance: {current_balance} USDT"
                logging.info(f"{response}")
                print(f"{dateandtime()} {response}")
                send_update(telegram_user, f"❌ {response}")
                config = configparser.ConfigParser()
                config.read('Binance_Trading.ini')
                config["DEFAULT"]['status'] = str(status + 1)
                with open('Binance_Trading.ini', 'w') as configfile:  # save
                    config.write(configfile)
    num_of_orders = len(myresult)
    logging.info(f"Number of open orders: {num_of_orders}")
    print(f"{dateandtime()} Number of open orders: {num_of_orders}")

def main():
    while True:
        #pprint.pprint(client.get_account())
        #logging.info(f"{client.get_account()}")
        #print(f"{dateandtime()} {client.get_account()}")
        coin = [{'symbol': 'ETHUSDT', 'quantity': 0.05}, {'symbol': 'LINKUSDT', 'quantity': 10}]
        account = client.get_account()
        balance = float(account['balances'][11]['free'])
        # coin = {'symbol': 'ETHUSDT', 'quantity': '10'}
        # symbol = coin['symbol']
        # quantity = coin['quantity']
        # quantity = input("Please input pair name: ")
        # symbol = input(f"Please quantity of {symbol}: ")
        for x in coin:
            logging.info(f"Pair Name: {x['symbol']} - Quantity: {x['quantity']} - Balance: {balance} USDT")
            print(f"{dateandtime()} Pair Name: {x['symbol']} - Quantity: {x['quantity']} - Balance: {balance} USDT")
            bollinger_trade_logic(balance, x['symbol'], x['quantity'])
        time.sleep(600)


if __name__ == "__main__":
    client = Client(api_key, api_secret, testnet=False)
    logging.info("Binance API Server")
    print(f"{dateandtime()} Binance API Server")
    main()
