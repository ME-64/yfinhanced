from yfinhanced import YFWSClient
from threading import Lock

lock = Lock()
data = {}
async def on_message(msg):
    with lock:
        if msg['symbol'] not in data.keys():
            data[msg['symbol']] = []
        data[msg['symbol']] += [msg]
    print(msg)

yfws = YFWSClient()

yfws.on_message = on_message

await yfws.connect()
await yfws.stop()
await yfws.subscribe('AAPL')

await yfws._thread.ai_submit(yfws.ws.send_json({'blllll': 'dfdf'}))
await yfws.unsubscribe('EURUSD=X')


from yfinhanced import YFClient

yf = YFClient()
await yf.connect()

x = await yf.get_quote('AAPL')
y = await yf.get_quote('AAPL')

x['bid'] - y['bid']


symbols = ['BYND', 'AAPL', 'BTC-USD', 'AMZN', 'TSLA', 'GOOG', 'BLNK',
        'RAD', 'SDC', 'FUV', 'WKHS', 'EVGO', 'VUZI', 'BBBY', 'SAVA', 'BIG', 'ENSV',
        'OCGN', 'MVIS', 'STAR', 'NVDA', 'MU', 'MRVL', 'ON', 'QCOM', 'COIN', 'DM', 'ZS',
        'SMAR', 'NOW', 'FOUR', 'ZS', 'DQ', 'BILL', 'AI','MCHP','WIT', 'HIMX', 'NET', 'UMC', 'XM', 'UCTT',
        'FROG', 'PAR', 'PAYC', 'DV', 'VSH', 'LSCC', 'ETH-USD', 'VTEX', 'KLIC', 'SHOP']

