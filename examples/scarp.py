from yfinhanced import YFClient
import pandas as pd
import numpy as np
import json

yf = YFClient()
await yf.connect()

x = await yf.get_quote('AAPL')

## CRYPTO
currencies = ['USD', 'GBP', 'EUR', 'BTC', 'ETH']
res = []
for c in currencies:
    tmp = await yf.get_crypto_reference(max_results=100, currency=c)
    res.append(tmp)

res = pd.concat(res, ignore_index=True)




## ETFs
regs = await yf._get_region_reference()
regs =[x['code'] for x in regs]

etf = []
for r in regs:
    tmp = await yf.get_etf_reference(region=r, max_results=10000)
    etf.append(tmp)

etf = pd.concat(etf, ignore_index=True)

payload = yf._build_screener_payload(region=['us'], mcap_filter=None, quote_type='ETF', sort_field='fundnetassets')
tst = await yf._send_screener_request(payload)
# res = await yf._iter_screener_requests(payload, 1)
# df = pd.DataFrame(res)





x = await yf.get_quote(['aapl', 'randomshit'], columns=['bid', 'ask'])
json.loads(x.to_json(orient='records'))

x = await yf.get_quote_summary('AAPL')
x = await yf.get_symbol_recos('BTC-USD')
x = await yf.get_esg_peer_scores('AAPL', count=30)


x = await yf.get_price_history('SPY', start=pd.to_datetime('2021-01-01', utc=True), 
        end=pd.to_datetime('today', utc=True), interval='1d', adjust=True)


{'size': 100,
 'offset': 0,
 'sortField': 'intradaymarketcap',
 'sortType': 'DESC',
 'quoteType': 'CRYPTOCURRENCY',
 'topOperator': 'AND',
 'query': {'operator': 'AND',
  'operands': [{'operator': 'AND',
    'operands': [{'operator': 'gt',
      'operands': ['lastclosemarketcap.lasttwelvemonths', 1]}]},
   {'operator': 'eq', 'operands': ['exchange', 'CCC']}],
  'userId': '',
  'userIdType': 'guid'}}
