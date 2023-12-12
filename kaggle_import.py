import pandas as pd
import psycopg2

db_params = {
    'host': 'localhost',
    'database': 'db_lab3',
    'user': 'postgres',
    'password': '1805',
    'port': '5432'
}

def import_data():
    rows_to_import = 50
    df = pd.read_csv('billionaires_dataset.csv', nrows=rows_to_import)

    df[['firstname', 'lastname']] = df['personName'].str.split(' ', n=1, expand=True)

    df['name_country'] = df['country']
    df['name_org'] = df['organization']
    df['category_org'] = df['category']

    clear_tables()

    insert_countries(df)
    insert_organizations(df)
    insert_billionaires(df)

def clear_tables():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    cursor.execute("DELETE FROM billionaire;")
    cursor.execute("DELETE FROM country;")
    cursor.execute("DELETE FROM organization;")

    connection.commit()

    cursor.close()
    connection.close()

def insert_billionaires(df):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    for index, row in df.iterrows():
        name_org = row['name_org'] if pd.notna(row['name_org']) else None
        if name_org is None:
            name_org = 'No Organization'

        cursor.execute("""
            INSERT INTO billionaire (firstname, lastname, gender, age, name_country, name_org)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;  -- Ігнорує конфлікт, якщо запис вже існує
        """, (row['firstname'], row['lastname'], row['gender'], row['age'], row['name_country'], name_org))

    connection.commit()

    cursor.close()
    connection.close()


def insert_countries(df):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    for country_info in df[['name_country', 'latitude_country', 'longitude_country']].itertuples(index=False):
        cursor.execute("""
            INSERT INTO country (name_country, latitude_country, longtitude_country)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM country WHERE name_country = %s);
        """, (*country_info, country_info.name_country))

    connection.commit()

    cursor.close()
    connection.close()

def insert_organizations(df):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    for org_info in df[['name_org', 'category_org']].drop_duplicates().itertuples(index=False):
        org_name = 'No Organization' if pd.isna(org_info.name_org) else org_info.name_org

        cursor.execute("""
            INSERT INTO organization (name_org, category_org)
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM organization WHERE name_org = %s);
        """, (org_name, org_info.category_org, org_name))

    connection.commit()

    cursor.close()
    connection.close()


if __name__ == '__main__':
    import_data()
