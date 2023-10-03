from importers.imports import *
from importers.commons import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

table_name = "mf_holdings_sample"
column_info = [("HoldDate","timestamp"),("TradeCode","varchar"),("ISIN","varchar"),("WithHeldHoldingQty","numeric"),("HoldingQty","numeric"),("ExchangeReceivableQty","numeric"),
               ("CollateralQty","numeric"),("TotalQty","numeric"),("ClosingRate","float"),("POA","char(3)"),("DPClientID","numeric primary key"),
               ("Haircut","float"),("Euser","varchar"),("LastUpdatedon","timestamp")]

unique_key_column = "DPClientID"

@app.post("/upload_master/upload_mf_holdings",tags=["masterData"])
def upload_mf_holdings(file = File(None)):
    if file.size>0:
        file_contents = file.file.read().decode()
        df = pd.read_csv(io.StringIO(file_contents))
        df_cleaned = df.dropna(how='any')
        df_list_of_list = df.values.tolist()
        column_names = [col[0] for col in column_info]
        upload_csv_DB_holdings(table_name=table_name,column_info=column_info,column_names=column_names,unique_key_column=unique_key_column.lower(),df=df_list_of_list)
        return {
            "message":"Holdings data from file uploaded successfully "
        }
    else:
        return {
                "message":"file not found"
            }
