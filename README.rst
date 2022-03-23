`yfinhanced` - A simple asyncronous wrapper around the public yahoo finance API
===============================================================================


Other packages either *a)* didn't expose the whole yahoo finance API and its
capabilities or *b)* were fully syncronous or threaded. `yfinhanced` solves both
of these issues by providing access to all endpoints, and while using pythons
modern asyncronous libraries such as `asycnio` and `aiohttp`.

.. code-block:: python3

    >>> from yfinhanced import YFClient
    >>> yf = YFClient()
    >>> await yf.connect()
    >>> quote = await yf.get_quote(['AAPL'])
    >>> print(quote)
    {'AAPL': {'language': 'en-US',
    'region': 'US',
    'quoteType': 'EQUITY',
    'typeDisp': 'Equity',
    'quoteSourceName': 'Nasdaq Real Time Price',
    'triggerable': True,
    'customPriceAlertConfidence': 'HIGH',
    'currency': 'USD',
    'exchange': 'NMS',
    'shortName': 'Apple Inc.',
    'longName': 'Apple Inc.',
    'messageBoardId': 'finmb_24937',
    'exchangeTimezoneName': 'America/New_York',
    'exchangeTimezoneShortName': 'EDT',
    'gmtOffSetMilliseconds': -14400000,
    'market': 'us_market',
    'esgPopulated': False,
    'fullExchangeName': 'NasdaqGS',
    'financialCurrency': 'USD',
    'regularMarketOpen': 167.99,
    'averageDailyVolume3Month': 93351350,
    'averageDailyVolume10Day': 97387620,
    'fiftyTwoWeekLowChange': 51.429993,
    'fiftyTwoWeekLowChangePercent': 0.43269387,
    'fiftyTwoWeekRange': '118.86 - 182.94',
    'fiftyTwoWeekHighChange': -12.650009,
    'fiftyTwoWeekHighChangePercent': -0.06914841,
    'fiftyTwoWeekLow': 118.86,
    'fiftyTwoWeekHigh': 182.94,
    'dividendDate': 1644451200,
    'earningsTimestamp': 1643301000,
    'earningsTimestampStart': 1651003200,
    'earningsTimestampEnd': 1651521600,
    'trailingAnnualDividendRate': 0.865,
    'trailingPE': 28.310888,
    'trailingAnnualDividendYield': 0.0051238,
    'epsTrailingTwelveMonths': 6.015,
    'epsForward': 6.56,
    'epsCurrentYear': 6.16,
    'priceEpsCurrentYear': 27.64448,
    'sharesOutstanding': 16319399936,
    'bookValue': 4.402,
    'fiftyDayAverage': 166.388,
    'fiftyDayAverageChange': 3.9019928,
    'fiftyDayAverageChangePercent': 0.023451166,
    'twoHundredDayAverage': 154.8563,
    'twoHundredDayAverageChange': 15.433701,
    'twoHundredDayAverageChangePercent': 0.099664666,
    'marketCap': 2779030487040,
    'forwardPE': 25.958841,
    'priceToBook': 38.68469,
    'sourceInterval': 15,
    'exchangeDataDelayedBy': 0,
    'firstTradeDateMilliseconds': 345479400000,
    'priceHint': 2,
    'regularMarketChange': 1.469986,
    'regularMarketChangePercent': 0.87074155,
    'regularMarketTime': 1648046328,
    'regularMarketPrice': 170.29,
    'regularMarketDayHigh': 170.76,
    'regularMarketDayRange': '167.65 - 170.76',
    'regularMarketDayLow': 167.65,
    'regularMarketVolume': 24929352,
    'regularMarketPreviousClose': 168.82,
    'bid': 170.08,
    'ask': 170.13,
    'bidSize': 12,
    'askSize': 8,
    'marketState': 'REGULAR',
    'pageViewGrowthWeekly': -0.10239728,
    'averageAnalystRating': '1.8 - Buy',
    'tradeable': False,
    'displayName': 'Apple'}}


