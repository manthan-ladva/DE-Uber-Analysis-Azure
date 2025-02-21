from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
from os import path
import pyodbc

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_mssql(data, **kwargs) -> None:
    """
    Template for exporting data to a MSSQL database.
    Specify your configuration settings in 'io_config.yaml'.
    Set the following in your io_config:

    Docs: https://docs.mage.ai/integrations/databases/MicrosoftSQLServer
    """

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for key,value in data.items():
        schema_name = 'dbo'  # Specify the name of the schema to export data to
        table_name = key  # Specify the name of the table to export data to
        
        with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                DataFrame(value),
                schema_name,
                table_name,
                index=False,  # Specifies whether to include index in exported table
                if_exists='replace',  # Specify resolution policy if table name already exists
            )