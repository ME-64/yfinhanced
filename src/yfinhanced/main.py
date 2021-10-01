import requests
import re
import pandas as pd
import urllib3
import time
import datetime
import pytz
from dateutil import relativedelta




class YFClient:

    BASE_URL = 'https://query1.finance.yahoo.com'# {{{
    SCREENER_URL = BASE_URL + '/v1/finance/screener'
    QUOTE_SUMMARY_URL = BASE_URL + '/v10/finance/quoteSummary/'
    TRENDING_URL = BASE_URL + '/v1/finance/trending/'
    FIELD_URL = BASE_URL + '/v1/finance/screener/instrument/{asset_class}/fields'
    QUOTE_URL = BASE_URL + '/v7/finance/quote' #params symbol
    RECCO_URL = BASE_URL + '/v6/finance/reccomendationsbysymbol/{symbol}'
    PEER_ESG_URL = BASE_URL + '/v1/finance/esgPeerScores' #params symbol
    QUOTE_TYP_URL = BASE_URL + '/v1/finance/quoteType' #params symbol
    SEARCH_URL = BASE_URL + '/v1/finance/search' # params q, quoteCount, newsCount, enableFuzzyQuery,
    HISTORY_URL = BASE_URL + '/v8/finance/chart/{symbol}'

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

    def connect(self):# {{{

        self.session = requests.session()


        retries = urllib3.util.retry.Retry(total=0,
                backoff_factor=6, status_forcelist=[429,500,502,503,504,413])

        ada = requests.adapters.HTTPAdapter(max_retries=retries)

        self.session.mount('https://', ada)
        self.session.headers.update(self.HEADERS)

        # we need to make a dummy request to get the crumb
        tmp = self.session.get(self.DUMMY_URL)

        crumb = re.findall('"CrumbStore":{"crumb":"(.+?)"}', tmp.text)[0]

        self.crumb = crumb 
        self.session.params.update({'crumb': self.crumb})
        # }}}

    def disconnect(self):# {{{
        self.session.close()# }}}

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

    def _send_screener_request(self, payload):# {{{

        print('sending screener request')
        resp = self.session.post(self.SCREENER_URL, json=payload)

        # data = resp.json()['finance']['result'][0]['quotes']
        try:
            result = resp.json()
        except:
            print(resp.content)

        if result['finance']['error']:
            print(payload)
            raise ValueError(f'{result["finance"]["error"]}')

        # return pd.DataFrame(data)
        return result['finance']['result'][0]
        # }}}

    def _iter_screener_requests(self, payload, max_results):# {{{

        if not max_results:
            max_results = 10000

        payload['size'] = 100
        payload['offset'] = 0

        total = 0


        data = []


        while True:
            result = self._send_screener_request(payload)
            data.extend(result['quotes'])

            if len(result['quotes']) < payload['size']:
                break

            if len(data) >= max_results:
                break
            new_offset = result['start'] + result['count']
            payload['offset'] = new_offset

        data = data[:max_results]

        return data# }}}

    def _get_quote_summary(self, ticker, modules=None):# {{{

        if isinstance(modules, str):
            modules = [modules]
        if not modules:
            modules = self.QUOTE_SUMMARY_MODULES

        url = self.QUOTE_SUMMARY_URL + ticker
        modules = ','.join(modules)
        params = {'modules': modules, 'format': False}

        res = self.session.get(url, params=params)

        res_js = res.json()['quoteSummary']['result'][0]
        return res_js# }}}

    def _get_trending(self, region='us', count=5):# {{{

        url = self.TRENDING_URL + '/' + region.upper()

        req = self.session.get(url, params={'count': count})

        return req.json()# }}}

    def get_equity_reference(self, region=['us'], max_results=10000, mcap_filter=100_000_000):# {{{

        payload = self._build_screener_payload(region=region, mcap_filter=mcap_filter)
        res = self._iter_screener_requests(payload, max_results)

        data = pd.DataFrame(res)

        cols = []
        for c in self.SCREENER_COLS:
            if c in data.columns:
                cols.append(c)
            else:
                print(f'column {c} not found in ref data')

        for c in data.columns:
            if c not in self.SCREENER_COLS:
                print(f'column {c} not found in defined screen columns')

        return data[cols]# }}}

    def _get_screener_fields(self, asset_class):# {{{

        url = self.FIELD_URL.format(asset_class=asset_class)

        req = self.session.get(url)


        flds = req.json()['finance']['result'][0]['fields']

        flds = pd.DataFrame(flds).T.reset_index(drop=True)

        return flds# }}}

    def _get_region_reference(self):# {{{

        flds = self._get_screener_fields('equity')

        regions = flds.loc[flds['fieldId']=='region']['labels'].iloc[0]

        clean_regions = []

        for i in regions:
            nme = i['displayName']
            value = i['criteria']['operands'][1]
            clean_regions.append(
                    {'name': nme,
                        'code': value})

        return clean_regions# }}}

    def _get_quote(self, symbols):# {{{

        if isinstance(symbols, str):
            symbols = [symbols]

        url = self.QUOTE_URL

        resp = self.session.get(url, params={'symbols': ','.join(symbols), 'fields': ','.join(self.QUOTE_COLUMNS)})

        resp = resp.json()
        if resp['quoteResponse']['error']:
            raise ValueError(f'{resp["quoteResponse"]["error"]}')

        return resp['quoteResponse']['result']# }}}

    def get_all_equity_reference(self):# {{{

        regions = self._get_region_reference()

        result = pd.DataFrame()

        for reg in regions:
            # filtering us more strictly than other regions
            # this is partly because of better mcap data coverage
            # and partly because there are far more securities ther
            if reg['code'] == 'us':
                mcap_filter = 100_000_000
            else:
                mcap_filter = None
            tmp = self.get_equity_reference(region=reg['code'], mcap_filter=mcap_filter)
            tmp['region_code'] = reg['code']
            tmp['region_name'] = reg['name']
            result = result.append(tmp, ignore_index=True)
            print(f'got data for {reg["name"]}')

        return result# }}}

    def get_quote(self, symbols, columns=None):# {{{

        if isinstance(columns, str):
            columns = [columns]

        data = self._get_quote(symbols)

        df = pd.DataFrame(data)

        if columns:
            if 'symbol' not in columns:
                columns += ['symbol']
            df = df[columns]

        return df# }}}

    def _hist_date_math(self, period, interval, start, end):# {{{
        """ NOT USED Helper function to get date ranges for historical data queries"""
        intra = ['1m','2m','5m','15m','30m','60m','90m','1h']

        intra = interval.lower() in intra


        if period:
            start, end = self._get_dates_from_period(period, end)
        else:

            # if it is a string, then convert to TZ
            if not isinstance(start, datetime.datetime):
                start = pd.to_datetime(start, utc=True)
            if not isinstance(end, datetime.datetime):
                end = pd.to_datetime(end, utc=True)

            # if it is already a datetime - then make sure there is a timezone
            if not start.tzinfo:
                start = pd.to_datetime(start, utc=True)
            elif start.tzinfo != pytz.UTC:
                start = start.tz_convert(pytz.UTC)
            if not end.tzinfo:
                end = pd.to_datetime(end, utc=True)
            elif end.tzinfo != pytz.UTC:
                end = end.tz_convert(pytz.UTC)


            # if just a date specified and start/end are the same - get the whole day (implied)
            if (interval.lower() in intra) and (start == end):
                start = start.replace(hour=0, minute=0, second=0)
                end = end.replace(hour=23, minute=59, second=59)

            # this is where you want to retrieve one datapoint per ticker
            if interval.lower() == '1d' and (start == end):
                end = end + pd.Timedelta(days=1)# }}}

    def _price_history(self, ticker, period=None, interval=None, start=None, end=None, adjust=True,# {{{
            prepost=False):

        url = self.HISTORY_URL.format(symbol=ticker)


        params = {'includeAdjustedClose': adjust, 'events': 'div,splits,capitalGain',
                'interval': interval, 'includePrePost': prepost}


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
        resp = self.session.get(url, params=params)

        resp = resp.json()
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

        return data# }}}

    def get_price_history(self, tickers, period=None, interval=None, start=None, end=None,# {{{
            adjust=True, prepost=False):

        if isinstance(tickers, str):
            tickers = [tickers]

        data = []
        for t in tickers:
            tmp = self._price_history(t, period=period, interval=interval, start=start,
                    end=end, adjust=adjust, prepost=prepost)
            data.append(tmp)

        return pd.concat(data)# }}}

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



# yf = YFClient()
# yf.connect()

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
