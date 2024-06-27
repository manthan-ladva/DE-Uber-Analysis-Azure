import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://storageamanthan.blob.core.windows.net/uber-de/uber_data.csv?sp=r&st=2024-06-23T13:39:27Z&se=2024-06-25T21:39:27Z&skoid=9cc7895e-7a0e-4f6c-a719-9566a6862ec1&sktid=18bb14fc-3a84-427e-a9aa-2f77c44e0a0a&skt=2024-06-23T13:39:27Z&ske=2024-06-25T21:39:27Z&sks=b&skv=2022-11-02&sv=2022-11-02&sr=b&sig=kRK6OuB%2BNrCbP8NgFMhNeXdZFFHG%2B7fYaxGARhJWN8Y%3D'
    response = requests.get(url)

    return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
