from yfinhanced import YFWSClient

data = {}
async def on_message(msg):
    with lock:
        if msg['symbol'] not in data.keys():
            data[msg['symbol']] = []
        data[msg['symbol']] += [msg]
    print(msg)

yfws = YFWSClient()

yfws.on_message = on_message

