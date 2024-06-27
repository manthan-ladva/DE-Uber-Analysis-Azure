import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(uber_df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    
    # Prerequired Transformation
    uber_df['tpep_pickup_datetime'] = pd.to_datetime(uber_df['tpep_pickup_datetime'])
    uber_df['tpep_dropoff_datetime'] = pd.to_datetime(uber_df['tpep_dropoff_datetime'])
    
    uber_df = uber_df.drop_duplicates().reset_index(drop=True)
    uber_df['trip_id'] = uber_df.index + 1

    # Dim Datetime
    dim_datetimes = uber_df[['tpep_pickup_datetime']].reset_index(drop=True)
    dim_datetimes['pick_hour'] = dim_datetimes['tpep_pickup_datetime'].dt.hour
    dim_datetimes['pick_day'] = dim_datetimes['tpep_pickup_datetime'].dt.day
    dim_datetimes['pick_month'] = dim_datetimes['tpep_pickup_datetime'].dt.month
    dim_datetimes['pick_year'] = dim_datetimes['tpep_pickup_datetime'].dt.year
    dim_datetimes['pick_weekday'] = dim_datetimes['tpep_pickup_datetime'].dt.weekday


    dim_datetimes['tpep_dropoff_datetime'] = uber_df[['tpep_dropoff_datetime']]
    dim_datetimes['drop_hour'] = dim_datetimes['tpep_dropoff_datetime'].dt.hour
    dim_datetimes['drop_day'] = dim_datetimes['tpep_dropoff_datetime'].dt.day
    dim_datetimes['drop_month'] = dim_datetimes['tpep_dropoff_datetime'].dt.month
    dim_datetimes['drop_year'] = dim_datetimes['tpep_dropoff_datetime'].dt.year
    dim_datetimes['drop_weekday'] = dim_datetimes['tpep_dropoff_datetime'].dt.weekday
    
    dim_datetimes['datetime_id'] = dim_datetimes.index + 1
    dim_datetimes = dim_datetimes[[dim_datetimes.columns[-1]] + dim_datetimes.columns[:-1].tolist()]

    # Dim Passengers Counts
    dim_passenger_counts = uber_df[['passenger_count']].reset_index(drop=True)
    dim_passenger_counts['passenger_count_id'] = dim_passenger_counts.index + 1
    dim_passenger_counts = dim_passenger_counts[['passenger_count_id', 'passenger_count']]

    # Dim Trip Distances
    dim_trip_distances = uber_df[['trip_distance']].reset_index(drop=True)
    dim_trip_distances['trip_distance_id'] = dim_trip_distances.index + 1
    dim_trip_distances = dim_trip_distances[['trip_distance_id', 'trip_distance']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    dim_rate_codes = uber_df[['RatecodeID']].reset_index(drop=True)
    dim_rate_codes['rate_code_id'] = dim_rate_codes.index + 1
    dim_rate_codes['name'] = dim_rate_codes['RatecodeID'].map(rate_code_type)
    dim_rate_codes = dim_rate_codes[['rate_code_id', 'name']]

    dim_pickup_locations = uber_df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    dim_pickup_locations['pickup_location_id'] = dim_pickup_locations.index + 1
    dim_pickup_locations = dim_pickup_locations[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    dim_drop_locations = uber_df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dim_drop_locations['drop_location_id'] = dim_drop_locations.index + 1
    dim_drop_locations = dim_drop_locations[['drop_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    dim_payment_types = uber_df[['payment_type']].reset_index(drop=True)
    dim_payment_types['payment_type_id'] = dim_payment_types.index + 1
    dim_payment_types['payment_type'] = dim_payment_types['payment_type'].map(payment_type_name)
    dim_payment_types = dim_payment_types[['payment_type_id', 'payment_type']]

    uber_fact_table = uber_df.merge(dim_datetimes, left_on='trip_id', right_on='datetime_id') \
                    .merge(dim_drop_locations, left_on='trip_id', right_on='drop_location_id') \
                    .merge(dim_passenger_counts, left_on='trip_id', right_on='passenger_count_id') \
                    .merge(dim_payment_types, left_on='trip_id', right_on='payment_type_id') \
                    .merge(dim_pickup_locations, left_on='trip_id', right_on='pickup_location_id') \
                    .merge(dim_rate_codes, left_on='trip_id', right_on='rate_code_id') \
                    .merge(dim_trip_distances, left_on='trip_id', right_on='trip_distance_id') \
                    [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                       'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'drop_location_id',
                       'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                       'improvement_surcharge', 'total_amount'
                     ]]

    uber_data_json = {'dim_datetimes':dim_datetimes.to_dict(orient="dict"),
        'dim_drop_locations':dim_drop_locations.to_dict(orient="dict"),
        'dim_passenger_counts':dim_passenger_counts.to_dict(orient="dict"),
        'dim_payment_types':dim_payment_types.to_dict(orient="dict"),
        'dim_pickup_locations':dim_pickup_locations.to_dict(orient="dict"),
        'dim_rate_codes':dim_rate_codes.to_dict(orient="dict"),
        'dim_trip_distances':dim_trip_distances.to_dict(orient="dict"),
        'uber_fact_table':uber_fact_table.to_dict(orient="dict")}


    return uber_data_json


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
