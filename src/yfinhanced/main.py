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

    BASE_URL = 'https://query1.finance.yahoo.com'# {{{
    SCREENER_URL = BASE_URL + '/v1/finance/screener'
    QUOTE_SUMMARY_URL = BASE_URL + '/v10/finance/quoteSummary/'
    TRENDING_URL = BASE_URL + '/v1/finance/trending/'
    FIELD_URL = BASE_URL + '/v1/finance/screener/instrument/{asset_class}/fields'
    QUOTE_URL = BASE_URL + '/v7/finance/quote' #params symbol
    RECCO_URL = BASE_URL + '/v6/finance/recommendationsbysymbol/{symbol}'
    PEER_ESG_URL = BASE_URL + '/v1/finance/esgPeerScores' #params symbol
    SEARCH_URL = BASE_URL + '/v1/finance/search' # params q, quoteCount, newsCount, enableFuzzyQuery,
    HISTORY_URL = BASE_URL + '/v8/finance/chart/{symbol}'
    TIME_URL = BASE_URL + '/v6/finance/markettime' # params region, lang=en-US

    DUMMY_URL = 'https://finance.yahoo.com/quote/AAPL'

    HEADERS = {'Content-Type': 'application/json',
            'Origin': 'https://finance.yahoo.com',
            'Accept-Language': 'en-gb',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15',
            }

    QUOTE_SUMMARY_MODULES = ['assetProfile', 'secFilings', 
            'incomeStatementHistory', 'cashflowStatementHistory', 'balanceSheetHistory',
            'incomeStatementHistoryQuarterly', 'cashflowStatementHistoryQuarterly', 'balanceSheetHistoryQuarterly',
            'earningsHistory', 'earningsTrend', 'industryTrend', 'indexTrend', 'sectorTrend',
            'esgScores', 'defaultKeyStatistics', 'calendarEvents', 'summaryProfile',
            'financialData', 'recommendationTrend', 'upgradeDowngradeHistory', 'earnings', 'details',
            'summaryDetail', 'price', 'pageViews', 'financialsTemplate', 'components',
            'institutionOwnership', 'fundOwnership', 'majorDirectHolders', 'insiderTransactions',
            'insiderHolders', 'majorHoldersBreakdown', 'netSharePurchaseActivity', 'fundPerformance']

    QUOTE_COLUMNS = ['symbol', 'uuid', 'underlyingExchangeSymbol', 'messageBoardId', 'longName',
            'shortName', 'marketCap', 'underlyingSymbol', 'headSymbolAsString', 'isin',
            'regularMarketPrice', 'regularMarketChange', 'regularMarketChangePercent', 
            'regularMarketVolume', 'regularMarketOpen', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
            'toCurrency', 'fromCurrency', 'toExchange', 'fromExchange', 'bid', 'ask', 'currency',
            'bidSize', 'askSize', 'averageDailyVolume3Month', 'averageDailyVolume10Day',
            'marketState', 'beta', 'preMarketTime', 'preMarketChange', 'preMarketChangePercent',
            'preMarketVolume', 'preMarketOpen', 'currentTradingPeriod', 'tradingPeriods',
            'preMarketPrice', 'preMarketDayHigh', 'preMarketDayLow', 'preMarketPreviousClose',
            'regularMarketPreviousClose', 'postMarketPreviousClose', 'postMarketVolume',
            'marketState', 'postMarketTime', 'regularMarketClose', 'last', 'trade',
            'postMarketChangePercent', 'postMarketPrice', 'postMarketChange'] #not used rn


    SCREENER_COLS = ['symbol', 'exchange', 'currency', 'fullExchangeName', 'quoteSourceName', 
            'shortName', 'longName', 'displayName', 'quoteType', 'firstTradeDateMilliseconds',
            'priceHint', 'market', 'messageBoardId', 'financialCurrency', 'sourceInterval', 'exchangeDataDelayedBy',
            'exchangeTimezoneName', 'exchangeTimezoneShortName', 'gmtOffSetMilliseconds', 'prevName', 'nameChangeDate']


# }}}

    def __init__(self):# {{{
        pass# }}}

    async def connect(self, *args, **kwargs):# {{{

        self.proxy = None
        if 'proxy' in kwargs:
            self.proxy = kwargs['proxy']

        self.crumb, self.cookies = self._get_crumb()

        connector = aiohttp.TCPConnector(limit=10)
        self.session = aiohttp.ClientSession(connector=connector,
                headers=self.HEADERS, cookies=self.cookies)


        # }}}

    def _get_crumb(self):# {{{
        """method to retrieve the crumb that yahoo uses for api authentication"""

        # tmp = requests.get(self.DUMMY_URL, headers=self.HEADERS, proxies=self.proxy)
        tmp = requests.get(self.DUMMY_URL, headers=self.HEADERS, proxies={'https': self.proxy})
        crumb = re.findall('"CrumbStore":{"crumb":"(.+?)"}', tmp.text)[0]
        cookies = tmp.cookies

        return crumb, cookies# }}}

    async def _make_request(self, method, url, *args, **kwargs):# {{{
        """function that makes all the requests async"""

        params = {'crumb': self.crumb}

        if 'params' in kwargs:
            params.update(kwargs['params'])
            del kwargs['params']

        resp = await self.session.request(method, url, params=params, proxy=self.proxy, *args, **kwargs)
        return resp# }}}

    async def disconnect(self):# {{{
        await self.session.close()# }}}

    def _build_screener_payload(self,# {{{
            sort_field='intradaymarketcap', sort_type='DESC', quote_type='EQUITY',
            mcap_filter=100_000_000, region='us'):

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

        print('sending')
        resp = await self._make_request('post', self.SCREENER_URL, json=payload)

        # data = resp.json()['finance']['result'][0]['quotes']
        try:
            result = await resp.json()
        except:
            cntnt = await resp.content
            print(cntnt)

        if result['finance']['error']:
            print(payload)
            raise ValueError(f'{result["finance"]["error"]}')

        # return pd.DataFrame(data)
        return result['finance']['result'][0]
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

            if len(result['quotes']) < payload['size']:
                break

            if len(data) >= max_results:
                break
            new_offset = result['start'] + result['count']
            payload['offset'] = new_offset

        data = data[:max_results]

        return data# }}}

    async def get_equity_reference(self, region=['us'], max_results=10000, mcap_filter=100_000_000):# {{{

        print(f'getting data for {region}')
        payload = self._build_screener_payload(region=region, mcap_filter=mcap_filter)
        res = await self._iter_screener_requests(payload, max_results)

        data = pd.DataFrame(res)

        cols = []
        for c in self.SCREENER_COLS:
            if c in data.columns:
                cols.append(c)
            else:
                pass
                # print(f'column {c} not found in ref data')

        for c in data.columns:
            if c not in self.SCREENER_COLS:
                pass
                # print(f'column {c} not found in defined screen columns')

        print(f'GOT data')
        return data[cols]# }}}

    async def get_all_equity_reference(self):# {{{

        regions = await self._get_region_reference()

        result = pd.DataFrame()

        tasks = []

        # regions = regions[0:20]

        for reg in regions:
            # filtering us more strictly than other regions
            # this is partly because of better mcap data coverage
            # and partly because there are far more securities ther
            if reg['code'] == 'us':
                mcap_filter = 100_000_000
            else:
                mcap_filter = None
            tmp = self.get_equity_reference(region=reg['code'], mcap_filter=mcap_filter)
            tasks.append(tmp)


        res = await asyncio.gather(*tasks)

        for reg, r in zip(regions, res):
            r['region_code'] = reg['code']
            r['region_name'] = reg['name']

        df = pd.concat([pd.DataFrame(x) for x in res])

        return df# }}}

    async def _get_quote_summary(self, ticker, modules=None):# {{{

        if isinstance(modules, str):
            modules = [modules]
        if not modules:
            modules = self.QUOTE_SUMMARY_MODULES

        url = self.QUOTE_SUMMARY_URL + ticker
        modules = ','.join(modules)
        params = {'modules': modules, 'format': 'false'}

        # res = self.session.get(url, params=params)
        res = await self._make_request('get', url, params=params)

        res_js = (await res.json())['quoteSummary']['result'][0]
        return res_js# }}}

    async def get_quote_summary(self, symbols, modules=None):# {{{

        if isinstance(symbols, str):
            symbols = [symbols]


        tasks = []

        for symb in symbols:
            tasks.append(self._get_quote_summary(symb, modules=modules))

        res = await asyncio.gather(*tasks)


        fres = {}
        for symb, qs in zip(symbols, res):
            fres[symb] = qs

        return fres# }}}

    async def _get_trending(self, region='us', count=5):# {{{
        print(f'getting {region}')

        url = self.TRENDING_URL + '/' + region.upper()

        req = await self._make_request('get', url, params={'count': count})

        resp = await req.json()

        try:
            data = resp['finance']['result'][0]['quotes']
            tmp = []
            for i in data:
                tmp.append(i['symbol'])
            print(f'got {region}')
            return tmp
        except Exception as e:
            print(resp)
            raise e
        # }}}

    async def _get_screener_fields(self, asset_class):# {{{

        url = self.FIELD_URL.format(asset_class=asset_class)

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

    async def _get_quote(self, symbols):# {{{

        print(f'getting data for {symbols}')

        if isinstance(symbols, str):
            symbols = [symbols]

        url = self.QUOTE_URL

        resp = await self._make_request('get', url, params={'symbols': ','.join(symbols), 'fields': ','.join(self.QUOTE_COLUMNS)})

        resp = await resp.json()
        if resp['quoteResponse']['error']:
            raise ValueError(f'{resp["quoteResponse"]["error"]}')

        print(f'GOT data')

        return resp['quoteResponse']['result']# }}}

    async def get_quote(self, symbols, columns=None):# {{{

        if isinstance(columns, str):
            columns = [columns]


        chunks = [symbols[x:x+50] for x in range(0, len(symbols), 50)]
        tasks = []
        for chunk in chunks:
            tasks.append(self._get_quote(chunk))

        res = await asyncio.gather(*tasks)

        df = pd.concat([pd.DataFrame(x) for x in res])

        if columns:
            if 'symbol' not in columns:
                columns += ['symbol']
            df = df[columns]

        return df# }}}

    async def _price_history(self, ticker, period=None, interval=None, start=None, end=None, adjust=True,# {{{
            prepost=False):

        print(f'getting price history for {ticker}')
        url = self.HISTORY_URL.format(symbol=ticker)


        params = {'includeAdjustedClose': str(adjust).lower(), 'events': 'div,splits,capitalGain',
                'interval': interval, 'includePrePost': str(prepost).lower()}


        assert isinstance(end, datetime.datetime)
        assert end.tz
        if period:
            start, end = self._get_dates_from_period(period, end)
        else:
            assert isinstance(start, datetime.datetime)
            assert start.tz

        params['period1'] = int(time.mktime(start.timetuple()))
        params['period2'] = int(time.mktime(end.timetuple()))



        # print(params)
        resp = await self._make_request('get', url, params=params)

        resp = await resp.json()
        if 'finance' in resp.keys():
            raise ValueError(f'{resp["finance"]["error"]}')
        if resp['chart']['error']:
            raise ValueError(f'{resp["chart"]["error"]}')

        if not resp['chart']['result'][0]['indicators']['quote'][0]:
            raise ValueError(f"no results found for period")

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

        data = data.reset_index().rename(columns={'index': 'date'})
        data['ticker'] = ticker
        data['interval'] = interval.lower()

        cols = list(data.columns)
        cols.remove('ticker')
        cols.remove('date')


        data = data[['ticker', 'date'] + cols]

        # now for pre and post items
        thours = self._get_session_from_tperiod(meta)

        data['date_local'] = data['date'].dt.tz_convert(thours['tz'])

        data['regular_flag'] = 'N'
        data['pre_flag'] = 'N'
        data['post_flag'] = 'N'

        data.loc[(data['date_local'].dt.time>=thours['regular_start']) &
                (data['date_local'].dt.time < thours['regular_end']), 'regular_flag'] = 'Y'

        data.loc[(data['date_local'].dt.time>=thours['pre_start']) &
                (data['date_local'].dt.time < thours['pre_end']), 'pre_flag'] = 'Y'

        data.loc[(data['date_local'].dt.time>=thours['post_start']) &
                (data['date_local'].dt.time< thours['post_end']), 'post_flag'] = 'Y'

        print(f'GOT for ticker')
        return data# }}}

    async def get_price_history(self, tickers, period=None, interval=None, start=None, end=None,# {{{
            adjust=True, prepost=False):

        if isinstance(tickers, str):
            tickers = [tickers]

        tasks = []
        for t in tickers:
            tmp = self._price_history(t, period=period, interval=interval, start=start,
                    end=end, adjust=adjust, prepost=prepost)
            tasks.append(tmp)
        res = await asyncio.gather(*tasks)

        return pd.concat(res)# }}}

    def _get_dates_from_period(self, period, asof=None):# {{{

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

        if 'd' in period.lower():
            start = asof - pd.Timedelta(days=int(period[:-1]))

        elif 'mo' in period.lower():
            start = asof - relativedelta.relativedelta(months=int(period[:-2]))

        elif 'w' in period.lower():
            start = asof - relativedelta.relativedelta(weeks=int(period[:-1]))

        elif 'y' in period.lower():
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

    async def get_trending(self, regions=['us', 'gb'], count=5):# {{{

        if isinstance(regions, str):
            regions = [regions]

        res = {}
        tasks = []
        for reg in regions:
            tasks.append(self._get_trending(region=reg, count=count))

        res = await asyncio.gather(*tasks)

        fres = {}
        for r, rs in zip(regions, res):
            fres[r] = rs


        return fres# }}}

    async def _get_symbol_recos(self, symbol):# {{{

        if isinstance(symbol, str):
            symbol = [symbol]

        res = await self._make_request('get', self.RECCO_URL.format(symbol=','.join(symbol)))
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

        return pd.concat([pd.DataFrame(x) for x in df])# }}}

    async def _get_markettime(self, region):# {{{

        res = await self._make_request('get', self.TIME_URL,
                params={'lang': 'en-US', 'region': region})

        return await res.json()# }}}

    async def get_markettime(self, regions=['ar', 'au', 'br', 'ca', 'us', 'cn', 'de', 'es',#{{{
        'fr', 'hk', 'in', 'it', 'jp', 'kr', 'mx', 'nz', 'sg', 'tw']):

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

            tmp = pd.Series({
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
                'pytz_name': t['timezone'][0]['$text']})

            ffres.append(tmp)

        return pd.concat(ffres, axis=1).T# }}}

    async def _get_esg_score(self, symbol, count=5):# {{{


        res = await self._make_request('get', self.PEER_ESG_URL, params={'symbol': symbol,
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

            list_res = res['esgPeerScores']['result'][0]['esgPeerScoresDocuments']

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

        params = {'q': query, 'quoteCount': quote_count, 'newsCount': news_count,
                'enableFuzzyQuery': str(fuzzy).lower()}

        res = await self._make_request('get', self.SEARCH_URL, params=params)

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



# implement search
# yf = YFClient()
# await yf.connect()


# s = await yf.get_search(['BYND', 'Tesla'], stype='news', count=200)
# 

# await yf.disconnect()
# 


# esg = await yf.get_esg_peer_scores(['AAPL', 'TSLA'], count=20)

# times = await yf.get_markettime()


# trend = await yf.get_trending(regions=['us', 'hk'], count=10)

# qs = await yf.get_quote(['AAPL', 'TSLA'])

# data = await yf.get_all_equity_reference()

# data = await yf.get_equity_reference(region='us', max_results=10)


# data = await yf.get_price_history(['AAPL', 'TSLA'], period='1mo', end=pd.to_datetime('today',utc=True), interval='1d')


# await yf.disconnect()
# 
# trend = yf.get_trending(regions=['us', 'gb'], count=10)


# x = yf._price_history('AAPL', period='1d', interval='1m')

# df= yf._price_history('GOOG', start=pd.to_datetime('2021-09-29 00:00:00', utc=True),
#             end=pd.to_datetime('2021-09-29 11:59:59', utc=True), interval='5m', prepost=True)



# start = pd.to_datetime('2021-09-01 00:00:00', utc=True)
# end = pd.to_datetime('2021-09-30 23:00:00', utc=True)
# 
# df = yf._price_history('^GSPC', start=start, end=end, interval='5m', prepost=False)
# 
# df['hour'] = df['date_local'].dt.hour
# df['minute'] = df['date_local'].dt.minute
# df['time'] = df['date_local'].dt.time

# x = yf.get_price_history('AZN.L', period='1mo', interval='1d', adjust=True)
# x = yf.get_price_history('AZN.L', period='1mo', interval='1d', adjust=True)

# x = yf.get_price_history('AAPL', start='20210505', end='20210505', interval='1d', adjust=True)

# ref = yf.get_all_equity_reference()


# quotes = yf._get_quote(test['symbol'].tolist())

# qs = yf._get_quote_summary('GME')

# q = yf.get_quote(['GBPUSD=X', 'EURUSD=X', 'AAPL'], ['bid', 'ask'])
# # q = yf.get_quote(['BTC-USD'])
# q['spread'] = ((q['ask'] - q['bid']) / ((q['ask'] + q['bid']) / 2)) * 10000
# q



# test = yf.get_equity_reference(region='us', max_results=100)

# df = yf.get_equity_reference(region='us', max_results=None)
# uk = yf.get_equity_reference(region='gb', max_results=None)
