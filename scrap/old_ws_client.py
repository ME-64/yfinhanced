import base64
import time
import json
import aiohttp
from google.protobuf.json_format import MessageToDict
import asyncio
from .yfinhanced_pb2 import YFWSMsgModel
from threaded_asyncio import ThreadedEventLoop
import concurrent.futures
from threading import Lock

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

        self.on_message = None
        self.on_error = None
        self.on_close = None
        self.raw_data = []
        self.subscribed = []
        self.last_msgs = {}
        self.raw_log_numb = raw_log_numb
        self._lock = Lock()
        # }}}

    async def subscribe(self, symbols):# {{{
        with self._lock:
            symbols = [symbols] if isinstance(symbols, str) else symbols
            symbols = [x.upper() for x in symbols]
            log.debug(f'subscribing to {symbols}')
            # concurrent.futures.wait([await self._thread.ai_submit(self.ws.send_json({'subscribe': symbols}))])
            await self._thread.ai_submit(self.ws.send_json({'subscribe': symbols}))
            self.subscribed += symbols
            self.subscribed = list(set(symbols))
        # }}}

    async def unsubscribe(self, symbols=None):# {{{
        with self._lock:
            if not symbols:
                symbols = self.subscribed
            symbols = [symbols] if isinstance(symbols, str) else symbols
            symbols = [x.upper() for x in symbols]
            log.debug(f'unsubscribing from {symbols}')
            await self._thread.ai_submit(self.ws.send_json({'unsubscribe': symbols}), wait=True)
            self.subscribed = [x for x in self.subscribed if x not in symbols]
        # }}}

    async def _start_connections(self):# {{{
            self.session = aiohttp.ClientSession()
            self.ws = await self.session.ws_connect(self.wss_url)# }}}

    async def restart(self):# {{{
        """reconnect and resubscribe to symols on close"""
        log.debug(f'trying to restart')
        old_subs = self.subscribed.copy()
        await self.stop()
        await self.connect()
        await self.subscribe(old_subs)
            # }}}

    async def _outer_run(self):# {{{
        log.debug(f'starting outer run')
        while True:
            if self.finished:
                break
            if not self.ws.closed:
                await self._inner_run()
            else:
                if self.rc_attempts <= 5:
                    with self._lock:
                        time.sleep(self.rc_attempts * 3)
                        self.rc_attempts += 1
                else:
                    with self._lock:
                        self.rc_attempts = 0 # reset
                        self._thread.stop()
                        await self.session.close()
                    break
                print('somehow disconnecting, trying to reconnect')
                self.ws = await self.session.ws_connect(self.wss_url)
                with self._lock:
                    self.rc_attempts = 0
                await self.subscribe(self.subscribed)
                # }}}

    async def _inner_run(self):# {{{
        log.debug('starting inner run')
        msg = await self.ws.receive()
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
                # }}}

    async def _run(self):# {{{

        log.debug('starting the receive msg loop')
        while True:
            msg = await self.ws.receive()
            if msg.type == aiohttp.WSMsgType.CLOSED:
                log.debug(f'close msg received {msg}')
                if self.on_close:
                    await self.on_close(msg)
                break

            if msg.type == aiohttp.WSMsgType.ERROR:
                if self.on_error:
                    await self.on_error(msg)
                log.debug(f'error {msg}')
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                res = self.parse_message(msg)
                if self.on_message:
                    await self.on_message(res)
                # }}}

    async def connect(self):# {{{
        with self._lock:
            self._thread = ThreadedEventLoop()
            self._thread.start()
            # we block until finished
            log.debug(f'connecting...')
            await self._thread.ai_submit(self._start_connections(), wait=True)
            await self._thread.ai_submit(self._outer_run())
        # await self._run(on_message, on_error, on_close) # }}}

    async def stop(self):# {{{
        log.debug('stopping...')
        self.finished = True
        await self._thread.ai_submit(self.session.close(), wait=True)
        self._thread.stop()
        self.subscribed = []
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

        with self._lock:
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

