# `yfinhanced` - A simple asyncronous wrapper around the public yahoo finance API

Other packages either a) didn't expose the whole yahoo finance API and its
capabilities or b) were fully syncronous or threaded. `yfinhanced` solves both
of these issues by providing access to all endpoints, and while using pythons
modern asyncronous libraries such as `asycnio` and `aiohttp`.

```python

from yfinhanced import YFClient

yf = YFClient()
await yf.connect()

quote = await yf.get_quote(['AAPL', 'TSLA'])

print(quote)

```


