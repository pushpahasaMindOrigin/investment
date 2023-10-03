from importers.imports import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection
"""
# Column names for the table
column_names = ["category_code","category_name","last_updt_dt_time"]

# Name of the CSV file to upload
csv_file = "mf_category.txt"
try:
    with open(csv_file, 'w', newline='') as file:
        pass
    print(f"Contents of '{csv_file}' have been cleared.")
except FileNotFoundError:
    print(f"File '{csv_file}' not found.")
# Name of the table to be created or used
table_name = "mf_category_master"

# Establish a database connection
connections = connection.connection_estb()
cursor = connections.cursor()

# Create the table if it doesn't exist
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (category_code varchar UNIQUE, category_name varchar,last_updt_dt_time varchar);"
cursor.execute(create_table_query)
connections.commit()

# Upload data from the CSV file into the table
with open(csv_file, 'r') as file:
    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV DELIMITER '|'", file)
    connections.commit()

print(f"Data uploaded from '{csv_file}' to '{table_name}' table successfully.")

"""
@app.post("/upload_master/upload_mf_category_copy",tags=["masterData"])
def upload_mf_category_copy(file = File(None)):
    if file.size>0:
        file_contents = file.file.read().decode()
        df = pd.read_csv(io.StringIO(file_contents), header=None)
        df_cleaned = df.dropna(how='any')
    # Unique key column name
    unique_key_column = "category_code"

    # Table name
    table_name = "mf_category_master"

    # List of column names
    column_names = ["category_code","category_name","last_updt_dt_time"]
    # Path to the text file with pipe-separated values
    psv_file = "mf_category.txt"


    # Establish a database connection
    connections = connection.connection_estb()
    cursor = connections.cursor()

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {unique_key_column} VARCHAR PRIMARY KEY,
        {', '.join([f'{col} VARCHAR' for col in column_names[1:]])}
    );
    """
    cursor.execute(create_table_query)
    df_list_of_list = df.values.tolist()
    df_result = [item[0] for item in df_list_of_list]
    for line in df_result:
        data = line.strip().split('|')
        if len(data) != len(column_names):
            print("Invalid data format. Skipping.")
            continue

        values = [data[column_names.index(col)] for col in column_names]

        # Create or update the row based on the unique key
        upsert_query = f"""
        INSERT INTO {table_name} ({', '.join(column_names)})
        VALUES ({', '.join(['%s' for _ in column_names])})
        ON CONFLICT ({unique_key_column})
        DO UPDATE
        SET {', '.join([f'{col} = EXCLUDED.{col}' for col in column_names[1:]])}"""
        cursor.execute(upsert_query, values)

        connections.commit()
    print("Data upserted successfully.")
    return {
        "message":"category data from file uploaded successfully "
    }

