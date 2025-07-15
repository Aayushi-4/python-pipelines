#load.py

from connect import read_config,get_db_connection

def load_email_to_sql(email_list):
    config = read_config()
    conn = get_db_connection(config)
    cursor = conn.cursor() 


    for email in email_list:#loops through each email in the provided list 
        sender = email['sender']#extracts data from thr list 
        receiver = email['receiver']
        cc = email['cc']
        subject = email['subject']
        body = email['body']
        url1 = email.get('attachment_1_url', None)#retrives s3 urls if exists if not none 
        url2 = email.get('attachment_2_url', None)
        #create table
        create_table_query = """
    CREATE TABLE Email_Communications (
        sender_name VARCHAR(255),
        receiver_name VARCHAR(255),
        cc VARCHAR(255),
        subject VARCHAR(255),
        body TEXT,
        attachment_1_url VARCHAR(500),
        attachment_2_url VARCHAR(500)
    )
    """
    #cursor.execute(create_table_query)

    insert_query = """
            INSERT INTO Email_Communications 
            (sender_name, receiver_name, cc, subject, body, attachment_1_url, attachment_2_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

    cursor.execute(insert_query, (sender, receiver, cc, subject, body, url1, url2))

    conn.commit()
    cursor.close()
    conn.close()
    print("All emails loaded into SQL server")