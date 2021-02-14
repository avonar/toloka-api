# toloka-api

## Usage example
```

In [2]: import toloka_api

In [3]: tap = toloka_api.TolokaClient(oauth_token=f'{your_token}', sandbox=True)

In [4]: tasks = await tap.get_all_tasks(pool_id)

In [5]: exit
```
