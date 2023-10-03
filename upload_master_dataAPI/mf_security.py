from importers.imports import *
from importers.commons import *
main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

# Table name
table_name = "mf_security_master"

column_info =[('Token', 'varchar'), ('Symbol', 'varchar'), ('Series', 'varchar'), ('InstrumentType', 'varchar'), ('MaximumPhysicalRedemptionQuantityLimit', 'float'), ('RTASchemeCode', 'varchar'), ('AMCSchemeCode', 'varchar'), ('SchmesDepositorydetails', 'varchar'),
('ISIN', 'varchar primary key'), ('Foliolength', 'varchar'), ('SecurityStatusNormalMarket', 'varchar'), ('EligibilityNormalMarket', 'bit(1)'), ('SecurityStatusOddLotMarket', 'varchar'), ('EligibilityOddLotMarket', 'bit(1)'), ('SecurityStatusSpotMarket', 'varchar'),
('EligibilitySpotMarket', 'bit(1)'), ('SecurityStatusAuctionMarket', 'varchar'), ('EligibilityAuctionMarket', 'bit(1)'), ('AMCCode', 'varchar'), ('CategoryCode', 'varchar'), ('SchemeName', 'varchar'), ('IssueRate', "varchar"), ('MinimumPhysicalAdditionalSubscriptionValueLimit', ' float(53)'),
('BuyNAVPrice', 'varchar'), ('SellNAVPrice', 'varchar'), ('RTAAgentCode', 'varchar'), ('ValueDecimalIndicator', 'varchar'), ('CategoryStartTime', 'varchar'), ('QuantityDecimalIndicator', 'varchar'), ('CategoryEndTime', 'varchar'), ('MinimumPhysicalFreshSubscriptionValueLimit', 'float(53)'),
 ('MaximumPhysicalRedemptionValueLimit', 'float(53)'), ('NFOENDDATE', 'varchar'), ('NFOstartdate', 'varchar'), ('NavDate', 'varchar'), ('NFOallotmentdate', 'varchar'), ('StEligibleParticipateInMarketIndex', 'bit(1)'), ('StEligibleAON', 'bit(1)'), ('StEligibleMinimumFill', 'bit(1)'),
 ('SecurityDepositoryMandatory', 'bit(1)'), ('SecDividend', 'bit(1)'), ('SecAllowDep', 'bit(1)'), ('SecAllowSell', 'bit(1)'), ('SecModCxl', 'bit(1)'), ('SecAllowBuy', 'bit(1)'), ('MinimumPhysicalRedemptionValueLimit ', 'varchar'), (' MinimumPhysicalRedemptionQuantityLimit', 'varchar'),
 ('Dividend', 'bit(1)'), ('Rights', 'bit(1)'), ('Bonus', 'bit(1)'), ('Interest', 'bit(1)'), ('AGM', 'bit(1)'), ('EGM', 'bit(1)'), ('Other', 'bit(1)'), ('LocalUpdatedDateAndTime', 'varchar'), ('DeleteFlag', 'char(1)'), ('Remark', 'varchar'), ('SipEligibility', 'char(1)'),
('MaximumPhysicalFreshSubscriptionValueLimit', 'float(53)'), ('MaximumPhysicalAdditionalSubsriptionValueLimit', 'float(53)'), ('MaximumDepositoryFreshSubscriptionValueLimit', 'float(53)'), ('MaximumDepositoryAdditionalSubsriptionValueLimit', 'float(53)'),
('MinimumDepositoryFreshSubsriptionValueLimit', 'float(53)'), ('MinimumDepositoryAdditionallSubsriptionValueLimit', 'float(53)'), ('MaximumDepositoryRedemptionQuantityLimit', 'float(53)'), ('MinimumDepositoryRedemptionQuantityLimit', 'float(53)'),
              ('MultipleForPhysicalSubsriptionLimit', 'float(53)'), ('MultipleForDepositorySubsriptionLimit', 'float(53)'), ('AMCName', 'varchar')]

# Unique key column name
unique_key_column = "ISIN"

#csv_to_tuple("upload_master_dataAPI/mf_security_headers.csv")
@app.post("/upload_master/upload_mf_security",tags=["masterData"])
def upload_mf_security(file = File(None)):
    if file.size>0:
        file_contents = file.file.read().decode()
        df = pd.read_csv(io.StringIO(file_contents), header=None)
        df_cleaned = df.dropna(how='any')
        df_list_of_list = df.values.tolist()
        df_result = [item[0] for item in df_list_of_list]
        column_names = [col[0] for col in column_info]
        upload_psv_DB(table_name=table_name,column_info=column_info,column_names=column_names,unique_key_column=unique_key_column.lower(),df=df_result[1:])
        return {
            "message":"Security data from file uploaded successfully "
        }
    else:
        return {
                "message":"file not found"
            }
