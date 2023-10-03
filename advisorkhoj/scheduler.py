from importers.imports import *
from advisorkhoj import all_schema_info

def job_that_executes_once():
    print("started....")
    all_schema_info.insert_schemes()
    return schedule.CancelJob

def scheduling():
    schedule.every().day.at('23:12').do(job_that_executes_once)

    while True:
        schedule.run_pending()
        time.sleep(1)


