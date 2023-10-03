from importers.imports import *
from importers.commons import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

table_name = "mf_transaction"
column_info = [("DateofReport","numeric"),("OrderNo","numeric primary key"),("SettlementType","varchar"),("SettlementNo","numeric"),("AllotmentMode","char(1)"),
               ("OrderDate","numeric"),("OrderTime","time"),("SchemeCategory","varchar"),
               ("AMCcode","varchar"),("AMCSchemeCode","varchar"),("RTACode","varchar"),("RTSchemeCode","varchar"),
               ("SchemeSymbol","varchar"),("SchemeSeries","varchar"),("SchemeOptionType","varchar"),
               ("ISINCode","varchar"),("OrderedQuantity","numeric"),("OrderedAmount","float"),("PurchaseType","numeric"),("MemberCode","numeric"),("BranchCode","numeric"),("DealerCode","numeric"),
               ("FolioNumber","numeric"),("PayoutMechanism","varchar"),("ApplicationNumber","numeric"),("ClientCode","varchar"),("TaxStatus","numeric"),("ModeofHolding","varchar"),("ClientName","varchar"),
               ("DepositoryName","varchar"),("DPId","numeric"),("DPclientId","numeric"),("NAVforallotment","float"),("Quantityallotted","float"),("Amountallotted","float"),("SIPRegdNo","numeric"),
               ("SIPTrancheNo","numeric"),("participantdpid","varchar"),("participantbeneficiaryid","varchar"),("EUINnumber","numeric"),("STAMPDUTY","float")]
unique_key_column = "OrderNo"

@app.post("/upload_master/upload_mf_transaction",tags=["masterData"])
def upload_mf_transaction(file = File(None)):
    if file.size>0:
        file_contents = file.file.read().decode()
        df = pd.read_csv(io.StringIO(file_contents), header=None)
        df_cleaned = df.dropna(how='any')
        df_list_of_list = df.values.tolist()
        df_result = [item[0] for item in df_list_of_list]
        column_names = [col[0] for col in column_info]
        upload_psv_DB(table_name=table_name,column_info=column_info,column_names=column_names,unique_key_column=unique_key_column.lower(),df=df_result[1:])
        return {
            "message":"Transaction data from file uploaded successfully "
        }
    else:
        return {
                "message":"file not found"
            }
