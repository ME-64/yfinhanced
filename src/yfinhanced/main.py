import requests
import re
import pandas as pd
import urllib3




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
            'insiderHolders', 'majorHoldersBreakdown', 'netSharePurchaseActivity']

    QUOTE_COLUMNS = ['symbol', 'uuid', 'underlyingExchangeSymbol', 'messageBoardId', 'longName',
            'shortName', 'marketCap', 'underlyingSymbol', 'headSymbolAsString', 'isin',
            'regularMarketPrice', 'regularMarketChange', 'regularMarketChangePercent', 
            'regularMarketVolume', 'regularMarketOpen', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh',
            'toCurrency', 'fromCurrency', 'toExchange', 'fromExchange', 'bid', 'ask', 'currency',
            'marketState', 'beta', 'preMarketTime', 'preMarketChange', 'preMarketChangePercent',
            'preMarketVolume', 'preMarketOpen', 'currentTradingPeriod', 'tradingPeriods',
            'preMarketPrice', 'preMarketDayHigh', 'preMarketDayLow', 'preMarketPreviousClose',
            'postMarketTime', 'regularMarketClose', 'last', 'trade'] #not used rn


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
            mcap_filter=100_000_000, region=None):

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
                        {
                            'operator': 'gt',
                            'operands': ['lastclosemarketcap.lasttwelvemonths', mcap_filter]
                        },
                 #        {
                 #            'operator': 'EQ',
                 #            'operands': ['isin', 'US0378331005']
                 #        },
                        ],
                'userId': '',
                'userIdType': 'guid'
                } }

        to_add = payload['query']['operands']

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
        result = resp.json()

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
        params = {'modules': modules}

        res = self.session.get(url, params=params)

        res_js = res.json()
        return res_js# }}}

    def _get_trending(self, region='us', count=5):# {{{

        url = self.TRENDING_URL + '/' + region.upper()

        req = self.session.get(url, params={'count': count})

        return req.json()# }}}

    def get_equity_reference(self, region=['us'], max_results=10000, mcap_filter=100_000_000):# {{{

        payload = yf._build_screener_payload(region=region, mcap_filter=mcap_filter)
        res = yf._iter_screener_requests(payload, max_results)

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

    def _get_quote(self, symbol):# {{{

        url = self.QUOTE_URL

        resp = self.session.get(url, params={'symbols': symbol, 'fields': ','.join(self.QUOTE_COLUMNS)})

        result = resp.json()['quoteResponse']['result']
        return result# }}}




yf = YFClient()
yf.connect()


aapl = yf._get_quote('vod.l')


# test = yf.get_equity_reference(region='us', max_results=100)

# df = yf.get_equity_reference(region='us', max_results=None)
# uk = yf.get_equity_reference(region='gb', max_results=None)
