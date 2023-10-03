from importers.imports import *
from advisorkhoj import save_apis_data

main_script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(main_script_directory)
from connect import connection

def insert_schemes():
    try:
        connections =connection.connection_estb()
        cursor = connections.cursor()
        create_table_query = sql.SQL(
            """CREATE TABLE IF NOT EXISTS mf_info_master
        (
            id	UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
          created_at timestamp DEFAULT now(),
          modified_at timestamp DEFAULT now(),
          created_by varchar,
          modified_by varchar,
          scheme_name varchar,
          nav float,
          nav_date varchar,
          nav_change float,
          nav_change_percentage float,
          scheme_inception_return float,
          benchmark_inception_return float,
          scheme_objective varchar,
          scheme_manager varchar,
          riskometer_value varchar,
          isin_no varchar UNIQUE,
          isin_divreinvst_no varchar,
          amc_logo varchar,
          scheme_category varchar,
          scheme_company varchar,
          scheme_company_short_name varchar,
          scheme_inception_date varchar,
          asset_class varchar,
          scheme_benchmark varchar,
          scheme_benchmark_code varchar,
          expense_ratio_percentage float,
          expense_ratio_date varchar,
          scheme_status varchar,
          minimum_investment float,
          minimum_topup float,
          scheme_assets float,
          scheme_asset_date varchar,
          is_dividend_scheme boolean,
          scheme_sip_start_date varchar,
          scheme_sip_end_date varchar,
          scheme_lumpsum_start_date varchar,
          scheme_performance_list JSON,
          risk_statistics_list JSON,
          scheme_peer_comparision_list JSON,
          schemeMapping JSON,
          schemePerformances JSON,
          peerComparisonResponse JSON,
          fundPerformanceOverviewAgainstBenchmarkAndCategoryResponse JSON,
          amc_details JSON,
          factsheet_name varchar,
          factsheet_link varchar,
          portfolio_name varchar,
          portfolio_link varchar,
          week_52_high_nav float,
          week_52_low_nav float,
          dividend_type varchar,
          week_52_high_date varchar,
          week_52_low_date varchar,
          amc_name varchar,
          one_year_return float,
          two_year_return float,
          three_year_return float,
          five_year_return float,
          ten_year_return float
                  )"""
        )
        cursor.execute(create_table_query)
        connections.commit()
        all_mfs_list = save_apis_data.read_api_data()
        failure_logs_file = 'advisorkhoj/logs/failure_logs_file.csv'
        success_logs_file = 'advisorkhoj/logs/success_logs_file.csv'
        try:
            with open(failure_logs_file, 'w', newline='') as file:
                pass
            print(f"Contents of '{failure_logs_file}' have been cleared.")
        except FileNotFoundError:
            print(f"File '{failure_logs_file}' not found.")

        try:
            with open(success_logs_file, 'w', newline='') as file:
                pass
            print(f"Contents of '{success_logs_file}' have been cleared.")
        except FileNotFoundError:
            print(f"File '{success_logs_file}' not found.")

        for each_scheme in all_mfs_list:
            each_scheme_details = {"created_by": "script", "modified_by": "script"}
            each_scheme_details.update(each_scheme)
            if each_scheme_details["status_msg"] == "Success":
                if "amc_details" in each_scheme_details:
                    each_scheme_details["amc_name"] = each_scheme_details["amc_details"]["amc_name"]
                if "one_year_return" in each_scheme_details["scheme_performance_list"][0]:
                    each_scheme_details["one_year_return"] = each_scheme_details["scheme_performance_list"][0]["one_year_return"]
                if "two_year_return" in each_scheme_details["scheme_performance_list"][0]:
                    each_scheme_details["two_year_return"] = each_scheme_details["scheme_performance_list"][0]["two_year_return"]
                if "three_year_return" in each_scheme_details["scheme_performance_list"][0]:
                    each_scheme_details["three_year_return"] = each_scheme_details["scheme_performance_list"][0]["three_year_return"]
                if "five_year_return" in each_scheme_details["scheme_performance_list"][0]:
                    each_scheme_details["five_year_return"] = each_scheme_details["scheme_performance_list"][0]["five_year_return"]
                if "ten_year_return" in each_scheme_details["scheme_performance_list"][0]:
                    each_scheme_details["ten_year_return"] = each_scheme_details["scheme_performance_list"][0]["ten_year_return"]

                if "nav_date" not in each_scheme_details:
                    each_scheme_details["nav_date"] = 'NULL'
                if "scheme_inception_date" not in each_scheme_details:
                     each_scheme_details["scheme_inception_date"] = 'NULL'
                if "expense_ratio_date" not in each_scheme_details:
                    each_scheme_details["expense_ratio_date"] = 'NULL'
                if "scheme_asset_date" not in each_scheme_details:
                    each_scheme_details["scheme_asset_date"] = 'NULL'
                if "scheme_sip_start_date" not in each_scheme_details:
                    each_scheme_details["scheme_sip_start_date"] = 'NULL'
                if "scheme_sip_end_date" not in each_scheme_details:
                    each_scheme_details["scheme_sip_end_date"] = 'NULL'
                if "scheme_lumpsum_start_date" not in each_scheme_details:
                    each_scheme_details["scheme_lumpsum_start_date"] = 'NULL'
                if "week_52_high_date" not in each_scheme_details:
                    each_scheme_details["week_52_high_date"] = 'NULL'
                if "week_52_low_date" not in each_scheme_details:
                    each_scheme_details["week_52_low_date"] = 'NULL'

                del each_scheme_details["status"]
                del each_scheme_details["status_msg"]
                del each_scheme_details["msg"]
                if "scheme_manager_biography" in each_scheme_details:
                    del each_scheme_details["scheme_manager_biography"]
                del each_scheme_details["riskometer_image"]
                del each_scheme_details["scheme_amfi_code"]
                if "amc_details" in each_scheme_details:
                    each_scheme_details["amc_details"] = json.dumps(each_scheme_details["amc_details"])
                if "schemeMapping" in each_scheme_details:
                    each_scheme_details["schemeMapping"] = json.dumps(each_scheme_details["schemeMapping"])
                if "schemePerformances" in each_scheme_details:
                    each_scheme_details["schemePerformances"] = json.dumps(each_scheme_details["schemePerformances"])
                if "fundPerformanceOverviewAgainstBenchmarkAndCategoryResponse" in each_scheme_details:
                    each_scheme_details["fundPerformanceOverviewAgainstBenchmarkAndCategoryResponse"] = json.dumps(each_scheme_details["fundPerformanceOverviewAgainstBenchmarkAndCategoryResponse"])
                if "scheme_performance_list" in each_scheme_details:
                    each_scheme_details["scheme_performance_list"] = json.dumps(each_scheme_details["scheme_performance_list"])#"array"+str([json.dumps(each_json) for each_json in each_scheme_details["scheme_performance_list"]])+" ::json[]"#f'({", ".join(map(str, each_scheme_details["scheme_performance_list"]))})'
                if "risk_statistics_list" in each_scheme_details:
                    each_scheme_details["risk_statistics_list"] = json.dumps(each_scheme_details["risk_statistics_list"])#"array"+str([json.dumps(each_json) for each_json in each_scheme_details["risk_statistics_list"]])+"::json[]"
                if "scheme_peer_comparision_list" in each_scheme_details:
                    each_scheme_details["scheme_peer_comparision_list"] = json.dumps(each_scheme_details["scheme_peer_comparision_list"])#"array"+str([json.dumps(each_json) for each_json in each_scheme_details["scheme_peer_comparision_list"]])+"::json[]"
                if "peerComparisonResponse" in each_scheme_details:
                    each_scheme_details["peerComparisonResponse"] = json.dumps(each_scheme_details["peerComparisonResponse"])#"array"+str([json.dumps(each_json) for each_json in each_scheme_details["peerComparisonResponse"]])+"::json[]"

                table_name = "mf_info_master"
                column_names = [string.lower() for string in list(each_scheme_details.keys())]
                #print(column_names)
                values_to_insert = list(each_scheme_details.values())
                #print(values_to_insert)
                unique_column_name = "isin_no" #insert based on unique isin_no
                insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s' for _ in values_to_insert])}) ON CONFLICT ({unique_column_name}) " \
                        f"DO UPDATE SET {', '.join([f'{name} = excluded.{name}' for name in column_names])}"
                cursor.execute(insert_query,values_to_insert)
                connections.commit()
                if cursor.rowcount > 0:
                    print(each_scheme_details["scheme_name"]+" inserted successfully in the database")
                else:
                    print("Query was not successful.")
                """
                with open(success_logs_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([each_scheme_details])
                """
            else:
                print("insertion failed")
                scheme_indices = [i for i, d in enumerate(all_mfs_list) if d == each_scheme]
                failed_scheme_link = save_apis_data.all_scheme_urls()[scheme_indices]

                with open(failure_logs_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([failed_scheme_link])

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")





