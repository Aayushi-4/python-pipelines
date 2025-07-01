from extract import connect_to_mongodb,extract_document
from load import connect_to_dynamo,load_to_dynamo

def main():
    print('connecting to mongo')
    con=connect_to_mongodb()

    print('extraction')
    df=extract_document()

    print('connect to dynamodb')
    conn=connect_to_dynamo()

    print('load to dynamo')
    load_df=load_to_dynamo(df,conn)
if __name__=="__main__":
    main
