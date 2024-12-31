
import pandas as pd
import psycopg2
import os

# Database connection parameters
host = "localhost"
database = "emaildb"
user = "postgres"
password = "roots"



try:
    # Establish a connection to the database
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Read data from CSV file
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, 'Book1.csv')

    data = pd.read_csv(file_path)
    #print(data.head())
    

    # Create table
    query = '''
        CREATE TABLE IF NOT EXISTS EmailSend (
            id SERIAL PRIMARY KEY,
            Email VARCHAR(255),
            Send VARCHAR(10)
        );
    '''
    cursor.execute(query)
    conn.commit()
    
    # Truncate table before inserting new data
    query = ''' TRUNCATE TABLE EmailSend RESTART IDENTITY;'''
    cursor.execute(query)
    conn.commit()


    # Insert data into table
    query = '''
        INSERT INTO EmailSend (Email, Send)
        VALUES (%s, %s);
    '''
    for index, row in data.iterrows():
        try:
            cursor.execute(query, (str(row['Email']), str(row['Send'])))
            conn.commit()
        except Exception as e:
            print(f"Error at row {index+1}: {e}")

    # Verify data migration
    query = "SELECT * FROM EmailSend;"
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)

except psycopg2.Error as e:
    print(f"Error: {e}")

finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()

