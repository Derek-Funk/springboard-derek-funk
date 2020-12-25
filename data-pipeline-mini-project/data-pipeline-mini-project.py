from mysql import connector

def get_db_connection(username, password, host, port, database):
    connection = None
    try:
        connection = connector.connect(user=username,
                                             password=password,
                                             host=host,
                                             port=port,
                                             database=database)
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

def create_sales_table(connection):
    sales_ddl = """
        CREATE TABLE sales(
            ticket_id INT,
            trans_date DATE,
            event_id INT,
            event_name VARCHAR(50),
            event_date DATE,
            event_type VARCHAR(10),
            event_city VARCHAR(20),
            event_addr VARCHAR(100),
            customer_id INT,
            price DECIMAL,
            num_tickets INT
        );
    """
    try:
        cursor = connection.cursor()
        cursor.execute(sales_ddl)
        cursor.close()
    except Exception as error:
        print("Error while creating sales table:", error)
    return

def load_third_party(connection, file_path_csv):
    try:
        cursor = connection.cursor()

        with open(file_path_csv) as f:
            # skipping event_addr as that is not in csv file
            sql_placeholder = "INSERT INTO sales VALUES (%s, '%s', %s, '%s', '%s', '%s', '%s', NULL, %s, %s, %s);"
            for line in f:
                formatted_line = tuple(line.strip().split(','))
                insert_sql = sql_placeholder % formatted_line
                cursor.execute(insert_sql)

        connection.commit()
        cursor.close()
    except Exception as error:
        print("Error while loading sales:", error)
    return

def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    try:
        sql_statement = "SELECT event_name FROM sales GROUP BY event_name ORDER BY SUM(price * num_tickets) DESC;"
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        cursor.close()
    except Exception as error:
        print("Error while querying sales table:", error)
    return records

def format_popular_tickets(tickets_list):
    print('Here are the most popular tickets in the past month (most popular on top):')
    for ticket in tickets_list:
        print('-', ticket[0])
    return

# connection = get_db_connection(username='root', password=<YOUR_MYSQL_PASSWORD>, host='localhost', port=3306, database=<YOUR_EXISTING_DATABASE>)
# create_sales_table(connection=connection)
# load_third_party(connection=connection, file_path_csv='/Users/derek-funk/Documents/data-engineering/unit-15-data-pipelines/data-pipeline-mini-project/third_party_sales_1.csv')
# tickets_ranked = query_popular_tickets(connection=connection)
# format_popular_tickets(tickets_ranked)
