from yfinhanced import YFClient
import pandas as pd

yf = YFClient()
await yf.connect()

# x = await yf.get_price_history(['AAPL', 'TSLA'], period='ytd', end=pd.to_datetime('2022-02-10', utc=True), interval='1d')

x = await yf.get_quote('AAPL,TSLA')

x = await yf.get_price_history('TSLA', start=pd.to_datetime('2022-02-15', utc=True), 
        end=pd.to_datetime('today', utc=True), interval='15m', adjust=True)

x = await yf.get_quote_summary(['AAPL', 'TSLA'])

