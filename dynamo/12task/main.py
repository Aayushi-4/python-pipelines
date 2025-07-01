from extract1 import connect_to_dynamo,extract_item
from transform import transform_data
from load import load_to_sql

def main():
    print("connecting to dynamo ")
    con=connect_to_dynamo()

    print("extracting the data")
    raw_df=extract_item(connect_to_dynamo())

    print("transforming the data")
    transformation=transform_data(connect_to_dynamo())

    print("loading data into sql server")
    load_to_sql()

    print("ETL processes completed")

if __name__=="__main__":
    main()