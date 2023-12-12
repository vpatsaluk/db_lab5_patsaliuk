import json
import psycopg2

db_params = {
    'host': 'localhost',
    'database': 'db_lab3',
    'user': 'postgres',
    'password': '1805',
    'port': '5432'
}

def export_all_tables_to_json(json_filename):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    tables = ['billionaire', 'country', 'organization']
    data = {}

    for table in tables:
        query = f"SELECT * FROM {table};"
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        table_data = []
        for row in rows:
            table_data.append(dict(zip(columns, row)))

        data[table] = table_data

    connection.close()

    with open(json_filename, 'w') as json_file:
        json.dump(data, json_file, indent=2)

if __name__ == '__main__':
    export_all_tables_to_json('exported_data.json')
