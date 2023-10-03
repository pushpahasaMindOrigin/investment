from importers.imports import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

def csv_to_tuple(csv_file):
    data_tuples = []
    #csv_file = "mf_security_headers.csv"
    # Open and read the CSV file
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)

        # Iterate through the rows in the CSV file
        for row in csv_reader:
            # Create a tuple from the row and append it to the list
            cleaned_row = tuple(cell.replace('"','') for cell in row)
            data_tuples.append(cleaned_row)
    #print(data_tuples)

def upload_psv_DB(table_name,column_info,column_names,unique_key_column,df):

    # Establish a database connection
    connections = connection.connection_estb()
    cursor = connections.cursor()

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col_name} {col_type}' for col_name, col_type in column_info])}
    );
    """
    cursor.execute(create_table_query)
    connections.commit()
    for line in df:
        # Split the line into fields using the pipe character
        data = line.split('|')
        # Replace empty fields with 'None'
        data = [None if field.strip() == '' else field for field in data]
        if len(data) != len(column_names):
            print("Invalid data format. Skipping.")
            #print(len(data))
            continue
        #print(line)
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

def upload_csv_DB_holdings(table_name,column_info,column_names,unique_key_column,df):

    # Establish a database connection
    connections = connection.connection_estb()
    cursor = connections.cursor()

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col_name} {col_type}' for col_name, col_type in column_info])}
    );
    """
    cursor.execute(create_table_query)
    connections.commit()
    for line in df:
        # Split the line into fields using the pipe character
        data = line
        # Replace empty fields with 'None'
        data = [None if field == '' else field for field in data]
        if len(data) != len(column_names):
            print("Invalid data format. Skipping.")
            continue
        print(data)
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


column_info =[('Token', 'bytea'), ('Symbol', 'char(10)'), ('Series', 'char(2)'), ('InstrumentType', 'bytea'), ('MaximumPhysicalRedemptionQuantityLimit', 'float'), ('RTASchemeCode', 'char(5)'), ('AMCSchemeCode', 'char(10)'), ('SchmesDepositorydetails', 'char(2)'),
('ISIN', 'char(12)'), ('Foliolength', 'bytea'), ('SecurityStatusNormalMarket', 'bytea'), ('EligibilityNormalMarket', 'bit(1)'), ('SecurityStatusOddLotMarket', 'bytea'), ('EligibilityOddLotMarket', 'bit(1)'), ('SecurityStatusSpotMarket', 'bytea'),
('EligibilitySpotMarket', 'bit(1)'), ('SecurityStatusAuctionMarket', 'bytea'), ('EligibilityAuctionMarket', 'bit(1)'), ('AMCCode', 'char(3)'), ('CategoryCode', 'char(5)'), ('SchemeName', 'char(75)'), ('IssueRate', "BYTEA"), ('MinimumPhysicalAdditionalSubscriptionValueLimit', ' float(53)'),
('BuyNAVPrice', 'bytea'), ('SellNAVPrice', 'bytea'), ('RTAAgentCode', 'char(10)'), ('ValueDecimalIndicator', 'bytea'), ('CategoryStartTime', 'bytea'), ('QuantityDecimalIndicator', 'bytea'), ('CategoryEndTime', 'bytea'), ('MinimumPhysicalFreshSubscriptionValueLimit', 'float(53)'),
 ('MaximumPhysicalRedemptionValueLimit', 'float(53)'), ('NFOENDDATE', 'bytea'), ('NFOstartdate', 'bytea'), ('NavDate', 'bytea'), ('NFOallotmentdate', 'bytea'), ('StEligibleParticipateInMarketIndex', 'bit(1)'), ('StEligibleAON', 'bit(1)'), ('StEligibleMinimumFill', 'bit(1)'),
 ('SecurityDepositoryMandatory', 'bit(1)'), ('SecDividend', 'bit(1)'), ('SecAllowDep', 'bit(1)'), ('SecAllowSell', 'bit(1)'), ('SecModCxl', 'bit(1)'), ('SecAllowBuy', 'bit(1)'), ('MinimumPhysicalRedemptionValueLimit ', 'bytea'), (' MinimumPhysicalRedemptionQuantityLimit', 'bytea'),
 ('Dividend', 'bit(1)'), ('Rights', 'bit(1)'), ('Bonus', 'bit(1)'), ('Interest', 'bit(1)'), ('AGM', 'bit(1)'), ('EGM', 'bit(1)'), ('Other', 'bit(1)'), ('LocalUpdatedDateAndTime', 'bytea'), ('DeleteFlag', 'char(1)'), ('Remark', 'char(25)'), ('SipEligibility', 'char(1)'),
('MaximumPhysicalFreshSubscriptionValueLimit', 'float(53)'), ('MaximumPhysicalAdditionalSubsriptionValueLimit', 'float(53)'), ('MaximumDepositoryFreshSubscriptionValueLimit', 'float(53)'), ('MaximumDepositoryAdditionalSubsriptionValueLimit', 'float(53)'),
('MinimumDepositoryFreshSubsriptionValueLimit', 'float(53)'), ('MinimumDepositoryAdditionallSubsriptionValueLimit', 'float(53)'), ('MaximumDepositoryRedemptionQuantityLimit', 'float(53)'), ('MinimumDepositoryRedemptionQuantityLimit', 'float(53)'),
              ('MultipleForPhysicalSubsriptionLimit', 'float(53)'), ('MultipleForDepositorySubsriptionLimit', 'float(53)'), ('AMCName', 'char(25)')]


