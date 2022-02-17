## A python wrapper around the yahoo finance API that leverages pandas DataFrames



# get_quote

- If yahoo error: will raise a valueerror
- If no data found for quote: will return empty df with all columns present
- if data found, will return one row in df for each ticker



from yfinhanced import YFClient
yf = YFClient()
await yf.connect()

x = await yf.get_quote('BTC-USD')
