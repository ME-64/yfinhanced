import base64
import signal
import time
import json
import aiohttp
from google.protobuf.json_format import MessageToDict
import asyncio
try:
    from .yfinhanced_pb2 import YFWSMsgModel
except:
    from yfinhanced_pb2 import YFWSMsgModel

import logging

log = logging.getLogger(__file__)
log.addHandler(logging.StreamHandler())
log.setLevel('DEBUG')

class YFWSClient:

    def __init__(self, raw_log_numb=100):# {{{
        self.wss_url = 'wss://streamer.finance.yahoo.com'
        # variables for reconnecting
        self.finished = False
        self.rc_attempts = 0

        self._started = asyncio.Event()
        self._finished = False

        self.on_message = None
        self.on_error = None
        self.on_close = None
        self.raw_data = []
        self.subscribed = []
        self.last_msgs = {}
        self.raw_log_numb = raw_log_numb
        # }}}

    async def _send_json(self, js):# {{{
        await self._started.wait()
        log.debug(f'sending {js}')
        await self.ws.send_json(js)
        log.debug('sent js')# }}}

    async def subscribe(self, symbols):# {{{
        symbols = [symbols] if isinstance(symbols, str) else symbols
        symbols = [x.upper() for x in symbols]
        log.debug(f'subscribing to {symbols}')
        # concurrent.futures.wait([await self._thread.ai_submit(self.ws.send_json({'subscribe': symbols}))])
        await self._send_json({'subscribe': symbols})
        self.subscribed += symbols
        self.subscribed = list(set(symbols))
        # }}}

    async def unsubscribe(self, symbols=None):# {{{
        if not symbols:
            symbols = self.subscribed
        symbols = [symbols] if isinstance(symbols, str) else symbols
        symbols = [x.upper() for x in symbols]
        log.debug(f'unsubscribing from {symbols}')
        await self._send_json({'unsubscribe': symbols})
        self.subscribed = [x for x in self.subscribed if x not in symbols]
        # }}}

    async def start(self):# {{{
        while True:
            log.debug('connecting...')
            self.session = aiohttp.ClientSession()
            self.ws = await self.session.ws_connect(self.wss_url)
            log.debug('connected!')
            log.debug('starting receive message loop...')
            self.rc_attempts = 0
            self._started.set()

            while not self.ws.closed:
                msg = await self.ws.receive()
                log.debug('message received...')
                if msg.type == aiohttp.WSMsgType.CLOSED:
                    log.debug(f'close msg received {msg}')
                    if self.on_close:
                        await self.on_close(msg)

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.on_error:
                        await self.on_error(msg)
                    log.debug(f'error {msg}')

                elif msg.type == aiohttp.WSMsgType.TEXT:
                    res = self.parse_message(msg)
                    if self.on_message and res:
                        await self.on_message(res)

            self._started.clear()
            if self.rc_attempts <=5 and not self._finished:
                log.debug(f'disconnected... attempting to re-establish...')
                await asyncio.sleep(self.rc_attempts * 3)
                self.rc_attempts += 1
                await self.session.close() # dropping conn before it resets
                # if we were subscribed to something, resubscribe
                if self.subscribed:
                    loop = asyncio.get_event_loop()
                    loop.create_task(self.subscribe(self.subscribed))
            else:
                log.debug('stopping the receive message loop...')
                self.subscribed = []
                await self.session.close()
                break # }}}

    async def stop(self):# {{{
        log.debug('disconnecting...')
        self._finished = True
        await self.ws.close()
        await self.session.close()
        # }}}

    def parse_message(self, msg):# {{{
        msg_bytes = base64.b64decode(msg.data)
        res = YFWSMsgModel()
        res.ParseFromString(msg_bytes)
        di = MessageToDict(res)

        # adjusting fields to align with names of rest api
        sess = di['marketState'].lower() + 'Market'
        for field in ['dayVolume', 'dayHigh', 'dayLow', 'change', 'openPrice', 'previousClose',
                'changePercent', 'price']:
            if field in di:
                tmp_val = di.pop(field)
                tmp_field = sess + field[0].upper() + field[1:]
                di[tmp_field] = tmp_val

        # first append to the raw data log
        self.raw_data.append(di)
        self.raw_data = self.raw_data[-self.raw_log_numb+1:] # deleting old records

        # then handle storing the current last known values
        sym_id = di.pop('id')
        if sym_id not in self.last_msgs:
            self.last_msgs[sym_id] = {}

        prev = self.last_msgs[sym_id].copy() # store state from pre message
        self.last_msgs[sym_id].update(di) # update state with new values
        current = self.last_msgs[sym_id] # now get post state message

        # work out delta and only show changes


        changed = {'symbol': sym_id}
        for k, v in current.items():
            tmp = prev.get(k, None)
            if tmp == v:
                continue
            else:
                changed[k] = v
        # if nothing has changed but the symbol, just return a blank
        if len(changed) <= 1:
            return
        else:
            return changed# }}}

    async def shutdown(self):# {{{
        log.debug('shutting down entirely')
        self._finished = True
        await self.stop()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        print('cancelling all tasks')
        await asyncio.gather(*tasks)
        log.debug('shutdown entirely')
        loop = asyncio.get_event_loop()
        loop.stop()
        # }}}

if __name__ == '__main__':
    yfws = YFWSClient()

    loop = asyncio.get_event_loop()
    loop.create_task(yfws.start())
    loop.create_task(yfws.subscribe('BTC-USD'))

    async def on_message(msg):
        print(msg)

    async def sub2(cb):# {{{
        await asyncio.sleep(5)
        await yfws.disconnect()
        await asyncio.sleep(3)
        loop = asyncio.get_event_loop()
        await asyncio.gather(yfws.start(), yfws.subscribe('AAPL'))# }}}

    yfws.on_message = on_message

    for s in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(yfws.shutdown()))

    try:
        loop.run_forever()
    finally:
        loop.close()
