import requests
import re
import pandas as pd
import urllib3
import time
import datetime
import pytz
from dateutil import relativedelta

import aiohttp
import asyncio



class YFClient:


    _BASE_URL: str = 'https://query1.finance.yahoo.com'# {{{
    _SCREENER_URL: str = _BASE_URL + '/v1/finance/screener'
    _QUOTE_SUMMARY_URL: str = _BASE_URL + '/v10/finance/quoteSummary/'
    _TRENDING_URL: str = _BASE_URL + '/v1/finance/trending/'
    _FIELD_URL: str = _BASE_URL + '/v1/finance/screener/instrument/{asset_class}/fields'
    _QUOTE_URL: str = _BASE_URL + '/v7/finance/quote' #params symbol
    _RECCO_URL: str = _BASE_URL + '/v6/finance/recommendationsbysymbol/{symbol}'
    _PEER_ESG_URL: str = _BASE_URL + '/v1/finance/esgPeerScores' #params symbol
    _SEARCH_URL: str = _BASE_URL + '/v1/finance/search' # params q, quoteCount, newsCount, enableFuzzyQuery,
    _HISTORY_URL: str = _BASE_URL + '/v8/finance/chart/{symbol}'
    _TIME_URL: str = _BASE_URL + '/v6/finance/markettime' # params region, lang=en-US

    _DUMMY_URL: str = 'https://finance.yahoo.com/quote/AAPL'

    _HEADERS: dict = {'Content-Type': 'application/json',
            'Origin': 'https://finance.yahoo.com',
            'Accept-Language': 'en-gb',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15',
            }

    _QUOTE_SUMMARY_MODULES: list = ['assetProfile', 'secFilings', 
            'incomeStatementHistory', 'cashflowStatementHistory', 'balanceSheetHistory',
            'incomeStatementHistoryQuarterly', 'cashflowStatementHistoryQuarterly', 'balanceSheetHistoryQuarterly',
            'earningsHistory', 'earningsTrend', 'industryTrend', 'indexTrend', 'sectorTrend',
            'esgScores', 'defaultKeyStatistics', 'calendarEvents', 'summaryProfile',
            'financialData', 'recommendationTrend', 'upgradeDowngradeHistory', 'earnings', 'details',
            'summaryDetail', 'price', 'pageViews', 'financialsTemplate', 'components',
            'institutionOwnership', 'fundOwnership', 'majorDirectHolders', 'insiderTransactions',
            'insiderHolders', 'majorHoldersBreakdown', 'netSharePurchaseActivity', 'fundPerformance',
            'topHoldings', 'fundProfile', 'quoteType']
# }}}

    async def connect(self, limit: int|None = 2) -> None:# {{{
        """
        Parameters
        ----------
        limit:
            The maximum number of simultaneously open connections. If none
            then there is no maximum limit
        """
        self._limit = limit

        self._crumb, self._cookies = self._get_crumb()

        connector = aiohttp.TCPConnector(limit=self._limit)
        self.session = aiohttp.ClientSession(connector=connector,
                headers=self._HEADERS, cookies=self._cookies)
        # }}}

    async def get_new_crumb(self) -> None:# {{{
        self._crumb: str
        self._cookies: requests.cookies.RequestsCookieJar
        self._crumb, self._cookies = self._get_crumb()
        await self.session.close()
        connector : aiohttp.TCPConnector = aiohttp.TCPConnector(limit=self._limit)
        self.session : aiohttp.ClientSession = aiohttp.ClientSession(connector=connector,
                headers=self._HEADERS, cookies=self._cookies)
        # }}}

    def _get_crumb(self) -> None:# {{{
        """method to retrieve the crumb that yahoo uses for api authentication"""

        # tmp = requests.get(self.DUMMY_URL, headers=self.HEADERS, proxies=self.proxy)
        tmp : requests.models.Response = requests.get(self._DUMMY_URL, headers=self._HEADERS)
        crumb: str = re.findall('"CrumbStore":{"crumb":"(.+?)"}', tmp.text)[0]
        cookies: requests.cookies.RequestsCookieJar = tmp.cookies

        return crumb, cookies# }}}

    async def _make_request(self, method: str, url: str, **kwargs) -> aiohttp.client_reqrep.ClientResponse:# {{{
        """function that makes all the requests async"""

        crum: dict = {'crumb': self._crumb}

        if 'params' in kwargs:
            kwargs['params'].update(crum)
        else:
            kwargs['params'] = crum

        resp: aiohttp.client_reqrep.ClientResponse = await self.session.request(method, url, **kwargs)
        return resp# }}}

    async def disconnect(self) -> None:# {{{
        """close the client session"""
        await self.session.close()# }}}

    async def _get_quote(self, symbols: list, fields: list) -> list:# {{{

        url: str = self._QUOTE_URL
        params: dict = {'symbols': ','.join(symbols)}
        if fields:
            params.update({'fields': ','.join(fields)})

        resp: aiohttp.client_reqrep.ClientResponse = await self._make_request('get', url, params=params)

        resp: dict = await resp.json()
        if resp['quoteResponse']['error']:
            raise ValueError(f'{resp["quoteResponse"]["error"]}')

        resp: list = resp['quoteResponse']['result']
        return resp
        # }}}

    async def get_quote(self, symbols: str|list, fields: str|list =None) -> dict:# {{{
        """
        Parameters
        ----------
        symbols:
            The yahoo finance symbols to get data for
        fields:
            The yahoo finance fields to get data for. If not specified, it
            will return all available fields for the given instrument

        Returns
        -------
        dict:
            A dictionary where the keys are the symbols, and the values are a dictionary
            of key value pairs for each field.
            Note: Even when a subset of fields is specified - the yahoo finance
            API will always return certain fields (i.e. timestamp, symbol)
        """

        fields = [fields] if isinstance(fields, str) else fields
        symbols = [symbols] if isinstance(symbols, str) else symbols


        chunks = [symbols[x:x+50] for x in range(0, len(symbols), 50)]
        tasks = []
        tasks = [self._get_quote(chunk, fields) for chunk in chunks]

        res = await asyncio.gather(*tasks)

        res = [inner for outer in res for inner in outer]

        fres = {}
        for r in res:
            sym = r.pop('symbol')
            fres[sym] = r

        return fres
        # }}}

    async def _price_history(self, symbol, interval=None, start=None, end=None, adjust=True,# {{{
            prepost=False):

        print(f'getting price history for {symbol}')

        assert isinstance(end, datetime.datetime); assert end.tzinfo
        assert isinstance(start, datetime.datetime); assert start.tzinfo

        url = self._HISTORY_URL.format(symbol=symbol)


        params = {'includeAdjustedClose': str(adjust).lower(), 'events': 'div,splits,capitalGain',
                'interval': interval, 'includePrePost': str(prepost).lower()}



        # put the start and end in (micro) seconds
        params['period1'] = int(time.mktime(start.timetuple()))
        params['period2'] = int(time.mktime(end.timetuple()))



        resp = await self._make_request('get', url, params=params)

        resp = await resp.json()
        if 'finance' in resp.keys():
            print('finance error')
            print(f'{resp["finance"]["error"]}')
            raise ValueError(resp['chart']['error'])
        if resp['chart']['error']:
            if resp['chart']['error']['description'][0:35] == 'Data doesn\'t exist for startDate = ':
                print("no results found for period")
                return
            else:
                print('chart error')
                print(f'{resp["chart"]["error"]}')
                raise ValueError(resp['chart']['error'])
                return

        if not resp['chart']['result'][0]['indicators']['quote'][0]:
            print(f"no results found for period")
            return

        meta = resp['chart']['result'][0]['meta']
        resp = resp['chart']['result'][0]


        timestamps = pd.to_datetime(resp['timestamp'], unit='s', utc=True)

        data = pd.DataFrame(resp['indicators']['quote'][0], index=timestamps)

        # adjusting other columns too
        if adjust and 'adjclose' in resp['indicators'].keys():
            data['adjclose'] = resp['indicators']['adjclose'][0]['adjclose']
            data['ratio'] = data['adjclose'] / data['close']
            data['adjopen'] = data['open'] * data['ratio']
            data['adjhigh'] = data['high'] * data['ratio']
            data['adjlow'] = data['low'] * data['ratio']
        # elif adjust and 'adjclose' not in resp['indicators'].keys():
        #     data['adjclose'] = data['close'].copy()
        #     data['ratio'] = 1
        #     data['adjopen'] = data['open'].copy()
        #     data['adjhigh'] = data['high'].copy()
        #     data['adjlow'] = data['low'].copy()


        data = data.reset_index().rename(columns={'index': 'date'})
        data['symbol'] = symbol
        data['interval'] = interval.lower()

        cols = list(data.columns)
        cols.remove('symbol')
        cols.remove('date')


        data = data[['symbol', 'date'] + cols]

        # now for pre and post items
        thours = self._get_session_from_tperiod(meta)

        data['date_local'] = data['date'].dt.tz_convert(thours['tz'])

        data['trading_period'] = None

        data.loc[(data['date_local'].dt.time>=thours['regular_start']) &
                (data['date_local'].dt.time < thours['regular_end']), 'trading_period'] = 'regular'

        data.loc[(data['date_local'].dt.time>=thours['pre_start']) &
                (data['date_local'].dt.time < thours['pre_end']), 'trading_period'] = 'pre'

        data.loc[(data['date_local'].dt.time>=thours['post_start']) &
                (data['date_local'].dt.time< thours['post_end']), 'trading_period'] = 'post'

        print(f'GOT for {symbol}')
        return data# }}}

    async def get_price_history(self, symbols, interval, start, end,# {{{
            adjust=True, prepost=False):
        """
        interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1h, 1d, 5d, 1wk, 1mo, 3mo
        adjust can only work for intraday
        todo: workout what adjustment intraday actually does do (find a symbol that has recent one)
        volume seems to only populate in normal market hours
        start and end must be datetimes with timezones

        if a ticker isn't found, it won't be returned in the data set
        if no tickers are found; empty dataset is returned
        """

        if isinstance(symbols, str):
            if ',' in symbols:
                symbols = symbols.split(',')
            else:
                symbols = [symbols]

        tasks = []
        for t in symbols:
            tmp = self._price_history(t, interval=interval, start=start,
                    end=end, adjust=adjust, prepost=prepost)
            tasks.append(tmp)
        res = await asyncio.gather(*tasks)

        fres = []
        for r in res:
            if r is not None:
                fres.append(r)

        if fres:
            return pd.concat(fres)
        else:
            return pd.DataFrame(columns=self.PRICE_HISTORY_COLS)
        # }}}

    def _build_screener_payload(self,# {{{
            sort_field='intradaymarketcap', sort_type='DESC', quote_type='EQUITY',
            mcap_filter=100_000_000, region='us', exchange=None, currency=None):

        if isinstance(region, str):
            region = [region]


        payload = {
                'size': 100,
                'offset': 0,
                'sortField': sort_field,
                'sortType': sort_type,
                'quoteType': quote_type,
                'topOperator': 'AND',
                # 'includeFields': ['ticker','isin', 'companyshortname', 'sector', 'neutral_count'],
                'query': {
                    'operator': 'AND',
                    'operands': [
                        # {
                        #     'operator': 'gt',
                        #     'operands': ['lastclosemarketcap.lasttwelvemonths', mcap_filter]
                        # },
                 #        {
                 #            'operator': 'EQ',
                 #            'operands': ['isin', 'US0378331005']
                 #        },
                        ],
                'userId': '',
                'userIdType': 'guid'
                } }

        to_add = payload['query']['operands']

        if mcap_filter:
            to_add.append({'operator': 'AND', 'operands': 
                [{'operator': 'gt', 'operands': ['lastclosemarketcap.lasttwelvemonths', mcap_filter]}]})

        if exchange:
            to_add.append({ "operator": "eq", "operands": [ "exchange", exchange] })

        if currency:
            to_add.append({ "operator": "eq", "operands": [ "currency", currency ] })

        if region:
           #  reg_query = {
           #          "operator": "or",
           #          "operands": [
           #              {
           #                  "operator": "EQ",
           #                  "operands": [
           #                      "region",
           #                      region
           #                      ]
           #                  }
           #              ]
           #          }
            reg_query = {
                    "operator": "or",
                    "operands": []}

            for reg in region:
                reg_query['operands'].append(
                        {'operator': 'EQ',
                            'operands': ['region', reg]})

            to_add.append(reg_query)
        return payload# }}}

    async def _send_screener_request(self, payload):# {{{

        # print(f"sending: {payload}")
        print('sending')
        resp = await self._make_request('post', self._SCREENER_URL, json=payload)

        # data = resp.json()['finance']['result'][0]['quotes']
        try:
            result = await resp.json()
        except:
            cntnt = await resp.text()
            # print(payload)
            raise ValueError(f'couldnt convert to json {cntnt}')

        if result['finance'].get('error', None):
            # print(payload)
            if result['finance']['error']['description'] == 'Invalid Crumb':
                await self.get_new_crumb()
                print('getting new crumb')
                return await self._send_screener_request(payload)
            else:
                raise ValueError(f'{result["finance"]["error"]}')

        # return pd.DataFrame(data)
        if not result['finance'].get('result'):
            raise ValueError(f'undefined error {result}')
        # print(result['finance']['start'], result['finance']['count'], result['finance']['total'])
        fres = result['finance']['result'][0]
        print(fres['start'], fres['count'], fres['total'], len(fres['quotes']))
        return fres
        # return result['finance']['result'][0]
        # }}}

    async def _iter_screener_requests(self, payload, max_results):# {{{

        if not max_results:
            max_results = 10000

        payload['size'] = 100
        payload['offset'] = 0

        total = 0


        data = []


        while True:
            result = await self._send_screener_request(payload)
            data.extend(result['quotes'])


            if result['count'] == 0:
                print(f'nothing returned now so breaking')
                print(f'len of data: {len(data)}; stated total: {result["total"]}')
                break
            # this doesnt currently work
            #if result['total'] <= len(data):
            #    print(f'result["total"] - all results have been returned')
            #    break
            # crypto screener some queries have less than this so commenting out. need to see if breaks equity
            #if len(result['quotes']) < payload['size']:
            #    print(f'result has less than {payload["size"]} items so breaking')
            #    break

            if len(data) >= max_results:
                print(f'data is now longer than {max_results} so breaking')
                break
            new_offset = result['start'] + result['count']
            if new_offset > 9900:
                print(f'offset of {new_offset} would be greater than max results so breaking')
                break
            payload['offset'] = new_offset

        data = data[:max_results]

        return data# }}}

    async def get_equity_reference(self, region=['us'], max_results=10000, mcap_filter=100_000_000):# {{{

        print(f'getting data for {region}')
        payload = self._build_screener_payload(region=region, mcap_filter=mcap_filter)
        res = await self._iter_screener_requests(payload, max_results)

        data = pd.DataFrame()
        data = pd.DataFrame(res)
        return data# }}}

    async def get_crypto_reference(self, currency='USD', max_results=100):# {{{
        payload = self._build_screener_payload(region=None, mcap_filter=None, quote_type='CRYPTOCURRENCY',
                exchange='CCC', currency=currency)
        res = await self._iter_screener_requests(payload, max_results)
        return pd.DataFrame(res)# }}}

    async def get_etf_reference(self, region=['us'], max_results=100):# {{{
        payload = self._build_screener_payload(region=region, quote_type='ETF', sort_field='fundnetassets', mcap_filter=None)
        res = await self._iter_screener_requests(payload, max_results)
        return pd.DataFrame(res)# }}}

    async def _get_quote_summary(self, ticker, modules=None):# {{{

        url = self._QUOTE_SUMMARY_URL + ticker
        modules = ','.join(modules)
        params = {'modules': modules, 'format': 'false'}

        # res = self.session.get(url, params=params)
        res = await self._make_request('get', url, params=params)

        res_js = await res.json()

        if res_js['quoteSummary']['result']:
            return res_js['quoteSummary']['result'][0]
        else:
            print(res_js['quoteSummary']['error'])
            return {} # }}}

    async def get_quote_summary(self, symbols, modules=None):# {{{

        if not modules:
            modules = self._QUOTE_SUMMARY_MODULES

        symbols = [symbols] if isinstance(symbols, str) else symbols
        modules = [modules] if isinstance(modules, str) else modules


        tasks = [self._get_quote_summary(symb, modules=modules) for symb in symbols]

        res = await asyncio.gather(*tasks)


        fres = {}
        for symb, qs in zip(symbols, res):
            if qs:
                fres[symb] = qs

        return fres# }}}

    async def _get_trending(self, region='us', count=5):# {{{
        print(f'getting {region}')

        url = self._TRENDING_URL + '/' + region.upper()

        req = await self._make_request('get', url, params={'count': count})

        resp = await req.json()

        try:
            data = resp['finance']['result'][0]['quotes']
            tmp = []
            for i in data:
                tmp.append(i['symbol'])
            return tmp
        except Exception as e:
            return []
        # }}}

    async def get_trending(self, regions=['us', 'gb'], count=5):# {{{

        regions = [regions] if isinstance(regions, str) else regions

        tasks = [self._get_trending(region=reg, count=count) for reg in regions]

        res = await asyncio.gather(*tasks)

        fres = {}
        for r, rs in zip(regions, res):
            if rs:
                fres[r] = rs
        return fres# }}}

    async def _get_screener_fields(self, asset_class):# {{{

        url = self._FIELD_URL.format(asset_class=asset_class)

        req = await self._make_request('get', url)


        flds = (await req.json())['finance']['result'][0]['fields']

        flds = pd.DataFrame(flds).T.reset_index(drop=True)

        return flds# }}}

    async def _get_region_reference(self):# {{{

        flds = await self._get_screener_fields('equity')

        regions = flds.loc[flds['fieldId']=='region']['labels'].iloc[0]

        clean_regions = []

        for i in regions:
            nme = i['displayName']
            value = i['criteria']['operands'][1]
            clean_regions.append(
                    {'name': nme,
                        'code': value})

        return clean_regions# }}}

    def _get_dates_from_period(self, period, asof=None):# {{{
        """valid are 1d, 1mo, 1w, 1y, ytd, mtd, qtd"""

        if not asof:
            asof = pd.to_datetime('today')

        # if it is a string, then convert to TZ
        if not isinstance(asof, datetime.datetime):
            asof = pd.to_datetime(asof, utc=True)

        # if it is already a datetime - then make sure there is a timezone
        if not asof.tzinfo:
            asof = pd.to_datetime(asof, utc=True)
        elif asof.tzinfo != pytz.UTC:
            asof = asof.tz_convert(pytz.UTC)

        if 'd' in period.lower() and period.lower() not in ['ytd', 'mtd', 'qtd']:
            start = asof - pd.Timedelta(days=int(period[:-1]))

        elif 'mo' in period.lower():
            start = asof - relativedelta.relativedelta(months=int(period[:-2]))

        elif 'w' in period.lower():
            start = asof - relativedelta.relativedelta(weeks=int(period[:-1]))

        elif 'y' in period.lower() and period.lower() not in ['ytd']:
            start = asof - relativedelta.relativedelta(years=int(period[:-1]))

        elif period.lower() == 'ytd':
            start = asof.replace(month=1, day=1)

        elif period.lower() == 'mtd':
            start = asof.replace(day=1)

        elif period.lower() == 'qtd':
            qtr = asof.quarter
            mnth = qtr * 3  - 2
            start = asof.replace(month=mnth, day=1)

        else:
            raise ValueError(f'period {period} for {asof} not valid')

        return start, asof# }}}

    def _get_session_from_tperiod(self, tperiod):# {{{

        all_periods = tperiod['currentTradingPeriod']
        exch_tz = tperiod['exchangeTimezoneName']

        def convert_sess(session, s_e):
            tme = pd.to_datetime(all_periods[session][s_e], unit='s', utc=True)
            tme = tme.tz_convert(exch_tz)
            return tme.time()


        return {
                'tz': exch_tz,
                'regular_start': convert_sess('regular', 'start'),
                'regular_end': convert_sess('regular', 'end'),
                'pre_start': convert_sess('pre', 'start'),
                'pre_end': convert_sess('pre', 'end'),
                'post_start': convert_sess('post', 'start'),
                'post_end': convert_sess('post', 'end')}# }}}

    async def _get_symbol_recos(self, symbol):# {{{

        if isinstance(symbol, str):
            symbol = [symbol]

        res = await self._make_request('get', self._RECCO_URL.format(symbol=','.join(symbol)))
        res = await res.json()
        return res# }}}

    async def get_symbol_recos(self, symbols):# {{{

        chunks = [symbols[x:x+50] for x in range(0, len(symbols), 50)]
        tasks = []
        for chunk in chunks:
            tasks.append(self._get_symbol_recos(chunk))

        res = await asyncio.gather(*tasks)

        fres = []
        for r in res:
            tmp = r['finance']['result']

            for smb in tmp:
                fres.append(smb)

        # df = pd.concat([pd.DataFrame(x) for x in res])

        df = []

        for r in fres:
            reco = r['recommendedSymbols']
            reco_symbs = [t['symbol'] for t in reco]
            reco_score = [t['score'] for t in reco]
            symb = [r['symbol']] * len(reco)
            df.append({'symbol': symb, 'reco_symbol': reco_symbs,
                'reco_score': reco_score})

        if df:
            return pd.concat([pd.DataFrame(x) for x in df])
        else:
            return pd.DataFrame(columns=['symbol', 'reco_symbol', 'reco_score'])
        # }}}

    async def _get_markettime(self, region):# {{{

        res = await self._make_request('get', self._TIME_URL,
                params={'lang': 'en-US', 'region': region})

        return await res.json()# }}}

    async def get_markettime(self, regions=['ar', 'au', 'br', 'ca', 'us', 'cn', 'de', 'es',#{{{
        'fr', 'hk', 'in', 'it', 'jp', 'kr', 'mx', 'nz', 'sg', 'tw', 'gb']):

        if isinstance(regions, str):
            regions = [regions]

        regions = [x.lower() for x in regions]

        tasks = []
        for reg in regions:
            tasks.append(self._get_markettime(reg))

        res = await asyncio.gather(*tasks)

        fres = {}
        for reg, tme in zip(regions, res):
            fres[reg] = tme

        ffres = []

        for nme, mkt in fres.items():

            try:
                t = mkt['finance']['marketTimes'][0]['marketTime'][0]
            except:
                print(mkt)
                print(f'couldnt find data for {nme}')
                continue

            tmp = {
                'name': nme,
                'short_yname': t['id'],
                'yname': t['name'],
                'y_mkt_id': t['yfit_market_id'],
                'close': t['close'],
                'msg': t['message'],
                'open': t['open'],
                'time': t['time'],
                'dst_active': t['timezone'][0]['dst'],
                'gmtoffset': t['timezone'][0]['gmtoffset'],
                'timezone_code': t['timezone'][0]['short'],
                'pytz_name': t['timezone'][0]['$text']}

            ffres.append(tmp)

        # ffres = pd.concat(ffres, axis=1).T
        for mkt in ffres:
            mkt['open'] = pd.to_datetime(mkt['open'])
            mkt['close'] = pd.to_datetime(mkt['close'])
            mkt['open'] = mkt['open'].tz_convert(mkt['pytz_name'])
            mkt['close'] = mkt['close'].tz_convert(mkt['pytz_name'])
            mkt['dst_active'] = True if mkt['dst_active'].lower() == 'true' else False

        return pd.DataFrame(ffres)# }}}

    async def _get_esg_score(self, symbol, count=5):# {{{


        res = await self._make_request('get', self._PEER_ESG_URL, params={'symbol': symbol,
            'count': count})

        return await res.json()# }}}

    async def get_esg_peer_scores(self, symbols, count=5):# {{{

        if isinstance(symbols, str):
            symbols = [symbols]

        tasks = []

        for sym in symbols:
            tasks.append(self._get_esg_score(sym, count))

        res = await asyncio.gather(*tasks)

        fres = {}

        for sym, r in zip(symbols, res):
            fres[sym] = r


        final = []
        for sym, res in fres.items():

            try:
                list_res = res['esgPeerScores']['result'][0]['esgPeerScoresDocuments']
            except:
                continue

            for d in list_res:
                tmp = {}
                tmp['ticker'] = sym
                tmp['peer_ticker'] = d['ticker']
                tmp['peer_name'] = d['companyshortname']
                tmp['esg_score'] = d['esgScore']['raw']
                tmp['environment_score'] = d['environmentScore']['raw']
                tmp['governance_score'] = d['governanceScore']['raw']
                tmp['social_score'] = d['socialScore']['raw']
                final.append(tmp)

        return pd.DataFrame(final, index=range(0, len(final)))# }}}

    async def _get_search(self, query, stype='quote', count=5, fuzzy=False):# {{{

        if stype.lower() == 'quote':
            news_count = 0
            quote_count = count
        elif stype.lower() == 'news':
            news_count = count
            quote_count = 0

        params = {'q': query, 'quotesCount': quote_count, 'newsCount': news_count,
                'enableFuzzyQuery': str(fuzzy).lower()}

        res = await self._make_request('get', self._SEARCH_URL, params=params)

        res = await res.json()

        if stype.lower() == 'quote':
            return self._parse_quote_search(res)
        elif stype.lower() == 'news':
            return self._parse_news_search(res) # }}}

    async def get_search(self, queries, stype='quote', count=5, fuzzy=False):# {{{

        if isinstance(queries, str):
            queries = [queries]

        tasks = []
        for q in queries:
            tasks.append(self._get_search(q, stype=stype, count=count, fuzzy=fuzzy))

        res = await asyncio.gather(*tasks)

        fres = []
        for q, res in zip(queries, res):
            res['query_string'] = q
            fres.append(res)

        return pd.concat(fres)# }}}

    def _parse_quote_search(self, res):# {{{

        res = res['quotes']

        return pd.DataFrame(res)# }}}

    def _parse_news_search(self, res):# {{{

        res = res['news']

        return pd.DataFrame(res)# }}}

