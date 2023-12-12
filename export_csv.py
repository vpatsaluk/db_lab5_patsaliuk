import pandas as pd
import psycopg2

db_params = {
    'host': 'localhost',
    'database': 'db_lab3',
    'user': 'postgres',
    'password': '1805',
    'port': '5432'
}

def export_table_to_csv(table_name, csv_filename):
    connection = psycopg2.connect(**db_params)
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, connection)
    df.to_csv(csv_filename, index=False)
    connection.close()

def export_all_tables():
    export_table_to_csv('billionaire', 'billionaire.csv')
    export_table_to_csv('country', 'country.csv')
    export_table_to_csv('organization', 'organization.csv')

if __name__ == '__main__':
    export_all_tables()
