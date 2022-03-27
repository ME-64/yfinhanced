from yfinhanced import YFClient
import pandas as pd
import numpy as np
import json

yf = YFClient()
await yf.connect()

x = await yf.get_quote('AAPL')


x = await yf.get_price_history('SPY', start=pd.to_datetime('2021-01-01', utc=True), 
        end=pd.to_datetime('today', utc=True), interval='1d', adjust=True)
