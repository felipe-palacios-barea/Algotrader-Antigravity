# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 17:57:30 2024

@author: Administrador
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.contract import Contract
import threading
import time
from datetime import datetime
import pandas as pd

class IBApi(EWrapper, EClient):
    def __init__(self, bot):
        EClient.__init__(self, self)
        self.bot = bot
        
    @iswrapper
    def error(self, reqId, errorCode, errorString):
        print(f"Error: {reqId}, {errorCode}, {errorString}")

    @iswrapper
    def nextValidId(self, orderId):
        print(f"Next Valid Order ID: {orderId}")

    @iswrapper
    def historicalData(self, reqId, bar):
        self.bot.on_bar_update(reqId, bar)

    @iswrapper
    def historicalDataEnd(self, reqId, start, end):
        print(f"Historical data end - ReqId: {reqId}, from {start} to {end}")
        self.bot.data_collected = True  # Indicate that data collection is complete

class Bot:
    def __init__(self, symbols):
        self.symbols = symbols  # List of symbols to collect data for
        self.data = {}  # Dictionary to store data for each symbol
        self.dataframes = {}  # Dictionary to store DataFrames for each symbol
        self.data_collected = False  # Flag to indicate if data collection is complete
        self.ib = IBApi(self)  # Pass reference of Bot to IBApi
        # Ensure the correct port (7497 for TWS, 7496 for IB Gateway)
        self.ib.connect("127.0.0.1", 7497, 1)
        ib_thread = threading.Thread(target=self.run_loop, daemon=True)
        ib_thread.start()
        time.sleep(1)
        
        self.collect_data()

    def run_loop(self):
        self.ib.run()

    def collect_data(self):
        for symbol in self.symbols:
            self.data[symbol] = []  # Initialize list to store data for this symbol
            contract = self.create_contract(symbol.upper(), "STK", "SMART", "USD")
            
            # Requesting historical data for the past 15 minutes
            end_time = (datetime.utcnow()).strftime("%Y%m%d %H:%M:%S")
            duration = "1 M"
            bar_size = "1 min"
            self.ib.reqHistoricalData(self.symbols.index(symbol), contract, end_time + " UTC", duration, bar_size, "TRADES", 1, 1, False, [])
            
            # Wait for data collection to complete
            while not self.data_collected:
                time.sleep(1)
            
            self.data_collected = False  # Reset flag for next symbol
        
        # After collecting data for all symbols, create DataFrames
        self.create_dataframes()

    def on_bar_update(self, reqId, bar):
        symbol = self.symbols[reqId]
        self.data[symbol].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def create_dataframes(self):
        for symbol, bars in self.data.items():
            columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            df = pd.DataFrame(bars, columns=columns)
            df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d  %H:%M:%S')  # Convert date to datetime
            df = df.sort_values('Date')  # Sort by date
            df = df.reset_index(drop=True)  # Reset the index
            self.dataframes[symbol] = df  # Store the DataFrame in the dictionary

        
    def create_contract(self, symbol, secType, exchange, currency):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

def main(symbols):
    bot = Bot(symbols)
    return bot  # Return the bot instance to access dataframes later

if __name__ == "__main__":