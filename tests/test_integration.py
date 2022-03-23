import yfinhanced
import pytest
import pandas as pd


# setup api client
@pytest.fixture# {{{
async def yf():
    yf = yfinhanced.YFClient()
    await yf.connect()
    yield yf
    await yf.disconnect() # }}}

async def test_quote(yf):# {{{
    # testing basic request
    res = await yf.get_quote('AAPL')
    assert res['AAPL']['shortName'] == 'Apple Inc.'
    assert res['AAPL']['financialCurrency'] == 'USD'
    assert res['AAPL']['quoteType'] == 'EQUITY'
    assert res['AAPL']['currency'] == 'USD'

    # testing a bullshit ticker
    res = await yf.get_quote('nonesenseasdf')
    assert not res

    # testing passing a list of quotes
    res = await yf.get_quote(['TSLA', 'AAPL'])
    assert 'AAPL' in res
    assert 'TSLA' in res

    res = await yf.get_quote('EURUSD=X', fields='bid')
    assert 'bid' in res['EURUSD=X'] 

    res = await yf.get_quote('AAPL', fields=['bid', 'ask'])
    assert 'bid' in res['AAPL'] 
    assert 'ask' in res['AAPL'] 

    res = await yf.get_quote('AAPL', fields=['bid', 'ask', 'notafield'])
    assert 'bid' in res['AAPL']
    assert 'ask' in res['AAPL']
    assert 'notafield' not in res['AAPL'] # }}}

async def test_quote_summary(yf):# {{{
    res = await yf.get_quote_summary('AAPL')
    assert 'AAPL' in res
    assert 'assetProfile' in res['AAPL']

    res = await yf.get_quote_summary('nonesenseasdf')
    assert 'nonesenseasdf' not in res

    res = await yf.get_quote_summary(['AAPL', 'TSLA'])
    assert 'AAPL' in res
    assert 'TSLA' in res

    res = await yf.get_quote_summary(['AAPL', 'TSLA'], ['assetProfile'])
    assert 'AAPL' in res
    assert 'TSLA' in res
    assert 'assetProfile' in res['AAPL']
    assert 'assetProfile' in res['TSLA']

    # }}}

async def test_trending(yf):# {{{

    res = await yf.get_trending('us', count=1)
    assert 'us' in res
    assert len(res['us']) == 1

    res = await yf.get_trending(['us', 'gb'], count=1)
    assert 'us' in res
    assert 'gb' in res

    res = await yf.get_trending('sddff', count=1)
    assert not res # }}}

async def test_search(yf):# {{{
    res = await yf.get_search('AAPL', stype='quote')
    assert res.shape[0] > 0
    assert res['query_string'][0] == 'AAPL'

    res = await yf.get_search('dsafdas', stype='quote')
    assert res.shape[0] == 0

    res = await yf.get_search('AAPL', stype='news')
    assert res['query_string'][0] == 'AAPL'
    assert res.shape[0] > 0

    res = await yf.get_search(['AAPL', 'TSLA'], stype='news')
    assert 'AAPL' in res['query_string'].tolist()
    assert 'TSLA' in res['query_string'].tolist()# }}}

async def test_markettime(yf):# {{{
    res = await yf.get_markettime('us')
    assert res['name'][0] == 'us'
    assert res['short_yname'][0] == 'us'
    assert res['pytz_name'][0] == 'America/New_York'

    res = await yf.get_markettime(['us', 'gb'])
    assert 'gb' in res['short_yname'].tolist()
    assert 'us' in res['short_yname'].tolist()# }}}

async def test_esg_peer_scores(yf):# {{{
    res = await yf.get_esg_peer_scores('AAPL')
    assert 'AAPL' in res['ticker'].tolist()

    res = await yf.get_esg_peer_scores('nonesense')
    assert res.shape[0] == 0

    res = await yf.get_esg_peer_scores(['AAPL', 'TSLA'])
    assert 'AAPL' in res['ticker'].tolist()
    assert 'TSLA' in res['ticker'].tolist()# }}}

async def test_symbol_recos(yf):# {{{
    res = await yf.get_symbol_recos('AAPL')
    assert 'AAPL' in res['symbol'].tolist()
    assert res.shape[0] > 0

    res = await yf.get_symbol_recos(['AAPL', 'TSLA'])
    assert 'AAPL' in res['symbol'].tolist()
    assert 'TSLA' in res['symbol'].tolist()

    res = await yf.get_symbol_recos('nonesense')
    assert res.shape[0] == 0# }}}

async def test_equity_reference(yf):# {{{

    res = await yf.get_equity_reference('us', 100)
    assert res.shape[0] == 100
    assert 'nameChangeDate' in res.columns.tolist()
    assert 'us_market' in res['market'].tolist()

    res = await yf.get_equity_reference('kasld;fj', 100)
    assert res.shape[0] == 0

    res = await yf.get_equity_reference(['us', 'gb'], 100)
    assert res.shape[0] == 100# }}}

def test_date_logic(yf):# {{{

    dts = yf._get_dates_from_period('1mo', asof='2021-01-01')
    assert dts[1] - dts[0] == pd.Timedelta(days=31)

    dts = yf._get_dates_from_period('1y', asof='2000-01-01')
    assert dts[0] == pd.Timestamp(year=1999, month=1, day=1, tz='UTC')# }}}






