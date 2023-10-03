import psycopg2

def connection_estb():
    try:
        connection = psycopg2.connect(
            host="ec2-15-207-139-66.ap-south-1.compute.amazonaws.com",
            database="investements",
            user="skanda",
            password="9632916893"
        )
        return connection
    except psycopg2.Error as e:
        return "Error: Could not connect to the database."

"""
                    column_names = [
                        "created_by",
                        "modified_by",
                        "scheme_name",
                          'nav',
                          'nav_date',
                          'nav_change',
                          'nav_change_percentage',
                          'scheme_inception_return',
                          'benchmark_inception_return',
                          'scheme_objective',
                          'scheme_manager',
                          "riskometer_value",
                          "isin_no",
                          "isin_divreinvst_no",
                          'amc_logo',
                          'scheme_category',
                          'scheme_company',
                          'scheme_company_short_name',
                          'scheme_inception_date',
                          'asset_class',
                          'scheme_benchmark',
                          'scheme_benchmark_code',
                          'expense_ratio_percentage',
                          'expense_ratio_date',
                          'scheme_status',
                          'minimum_investment',
                          'minimum_topup',
                          'scheme_asset',
                          'scheme_asset_date',
                          'is_dividend_scheme',
                          'scheme_sip_start_date',
                          'scheme_sip_end_date',
                          'scheme_lumpsum_start_date',
                          'scheme_performance_list',
                          'risk_statistics_list',
                          'scheme_peer_comparision_list',
                          'schemeMapping_json',
                          "schemePerformances_json",
                          'peerComparisonResponse',
                          'fundPerformanceOverviewAgainstBenchmarkAndCategoryResponse_json',
                          'amc_details_json',
                          'factsheet_name',
                          'factsheet_link',
                          'portfolio_name',
                          "portfolio_link",
                          'week_52_high_nav',
                          'week_52_low_nav',
                          'dividend_type',
                          "week_52_high_date",
                          "week_52_low_date",
                          "amc_name",
                          "one_year_return",
                          "two_year_return",
                          "three_year_return",
                          "five_year_return",
                          "ten_year_return"
                        ]
"""
