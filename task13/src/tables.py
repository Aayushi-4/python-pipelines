from sqlalchemy.types import Integer, String,BigInteger
from connect import get_sqlalchamey_engine,read_config
import pandas as pd
from sqlalchemy import create_engine

connection=get_sqlalchamey_engine(read_config())
dtype = {
    'customer_Id': Integer(),
    'name': String(100),
    'email':String(50),
    'address': String(255),
    'phone': BigInteger()
}

df = pd.DataFrame({
    'customer_Id': [1,2,3,4,5],
    'name': ['John Doe','Jane smith','mike jhonson','emilydavis','davis lee'],
    'email':['john.doe@example.com','jane.smith@example.com','mike.johnson@example.com','emily.davis@example.com','david.lee@example.com'],
    'address': ['123 Main St, New York','456 Oak Ave, Los Angeles','789 Pine Rd, Chicago','321 Maple St, Houston','654 Elm Dr, Miami'],
    'phone': [9876543210,8765432109,7654321098,6543210987,5432109876]
})
df.to_sql('customers', con=connection, if_exists='replace', index=False, dtype=dtype)