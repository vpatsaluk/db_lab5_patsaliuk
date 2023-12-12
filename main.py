import psycopg2
from tabulate import tabulate
import matplotlib.pyplot as plt

db_params = {
    'host': 'localhost',
    'database': 'db_lab3',
    'user': 'postgres',
    'password': '1805',
    'port': '5432'
}


views_creation_queries = [
    '''
    CREATE OR REPLACE VIEW us_billionaires_categories AS
    SELECT DISTINCT o.category_org
    FROM Billionaire b
    JOIN organization o ON b.name_org = o.name_org
    JOIN country c ON b.name_country = c.name_country
    WHERE c.name_country = 'United States';
    ''',

    '''
    CREATE OR REPLACE VIEW billionaires_by_country AS
    SELECT c.name_country, COUNT(*) AS billionaire_count
    FROM Billionaire b
    JOIN Country c ON b.name_country = c.name_country
    GROUP BY c.name_country;
    ''',

    '''
    CREATE OR REPLACE VIEW billionaire_coordinates AS
    SELECT b.firstname, b.lastname, c.latitude_country, c.longtitude_country
    FROM Billionaire b
    JOIN Country c ON b.name_country = c.name_country;
    '''
]

def execute_queries(cursor, queries):
    for query in queries:
        cursor.execute(query)

def execute_query(cursor, query):
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def visualize_bar_chart(labels, values, title, xlabel, ylabel):
    plt.figure(figsize=(10, 15))
    plt.bar(labels, values, width=0.8)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(range(len(labels)), labels, rotation=45)
    plt.show()

def visualize_pie_chart(labels, sizes, title):
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.show()

def visualize_scatter_plot(data, title):
    latitudes = [row[2] for row in data]
    longitudes = [row[3] for row in data]

    plt.scatter(longitudes, latitudes, label='Мільярдери')
    plt.title(title)
    plt.xlabel('Довгота')
    plt.ylabel('Широта')
    plt.legend()
    plt.show()

def print_query_results(query, result, cursor):
    print(f"\nQuery: {query}\n")
    headers = [desc[0] for desc in cursor.description]
    print(tabulate(result, headers, tablefmt="pretty"))


def visualize_all_charts(result_1, result_2, result_3):
    fig, axs = plt.subplots(3, figsize=(10, 15))

    # Перший графік (гістограма)
    categories_1 = [row[0] for row in result_1]
    counts_1 = [result_1.count(row) for row in result_1]
    axs[0].bar(categories_1, counts_1, width=0.8)
    axs[0].set_title('Назви категорій, власники з США')
    axs[0].set_xlabel('Category')
    axs[0].set_ylabel('')

    # Другий графік (кругова діаграма)
    countries = [row[0] for row in result_2]
    counts_2 = [row[1] for row in result_2]
    axs[1].pie(counts_2, labels=countries, autopct='%1.1f%%', startangle=140)
    axs[1].set_title('Відсоткове представлення мільярдерів по країнах')

    # Третій графік (точкова діаграма)
    latitudes = [row[2] for row in result_3]
    longitudes = [row[3] for row in result_3]
    axs[2].scatter(longitudes, latitudes, label='Мільярдери')
    axs[2].set_title('Мільярдери та їхні координати')
    axs[2].set_xlabel('Довгота')
    axs[2].set_ylabel('Широта')
    axs[2].legend()

    plt.tight_layout()
    plt.show()

def main():
    connection = psycopg2.connect(
        user=db_params['user'],
        password=db_params['password'],
        dbname=db_params['database'],
        host=db_params['host'],
        port=db_params['port']
    )

    with connection.cursor() as cursor:
        execute_queries(cursor, views_creation_queries)

        result_1 = execute_query(cursor, 'SELECT * FROM us_billionaires_categories;')
        result_2 = execute_query(cursor, 'SELECT * FROM billionaires_by_country;')
        result_3 = execute_query(cursor, 'SELECT * FROM billionaire_coordinates;')

        visualize_all_charts(result_1, result_2, result_3)

    connection.close()

if __name__ == '__main__':
    main()