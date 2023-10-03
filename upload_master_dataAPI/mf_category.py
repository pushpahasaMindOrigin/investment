from importers.imports import *
from importers.commons import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

table_name = "mf_category_master"
column_info = [("category_code","varchar primary key"),("category_name","char(35)"),("last_updt_dt_time","numeric")]
unique_key_column = "category_code"

@app.post("/upload_master/upload_mf_category",tags=["masterData"])
def upload_mf_category(file = File(None)):
    if file.size>0:
        file_contents = file.file.read().decode()
        df = pd.read_csv(io.StringIO(file_contents), header=None)
        df_cleaned = df.dropna(how='any')
        df_list_of_list = df.values.tolist()
        df_result = [item[0] for item in df_list_of_list]
        column_names = [col[0] for col in column_info]
        upload_psv_DB(table_name=table_name,column_info=column_info,column_names=column_names,unique_key_column=unique_key_column.lower(),df=df_result)
        return {
            "message":"Category data from file uploaded successfully "
        }
    else:
        return {
                "message":"file not found"
            }
