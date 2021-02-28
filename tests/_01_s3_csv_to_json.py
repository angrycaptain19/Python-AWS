import awswrangler as wr
# import pandas as pd
# import boto3
# import pytz
# import json


def lambda_handler(event, context):

    # path with the source csv in S3
    source_path = 's3://firstestdemo/source/renewable-energy-stock-account-2007-18.csv'

    # read the csv file from S3 - dataframe output
    renewable_energy = wr.s3.read_csv([source_path])

    # delete unnecessary column from the DF
    del renewable_energy['flag']

    # DF calculation - value by year and resource
    value_by_year = renewable_energy.groupby(['year', 'resource'])['data_value'] \
        .sum() \
        .reset_index() \
        .sort_values(['year', 'resource'])

    # file output section
    # output paths - targets
    target_path1 = 's3://firstestdemo/target/renewable_energy.json'
    target_path2 = 's3://firstestdemo/target/value_by_year.json'
    # to_json command for each file
    wr.s3.to_json(renewable_energy, target_path1)
    wr.s3.to_json(value_by_year, target_path2)

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }


if __name__ == '__main__':

    lambda_handler(None, None)

