from upload_master_dataAPI import mf_category,mf_security,mf_holdings,mf_transaction
from advisorkhoj import scheduler
from importers.imports import *

#scheduler.scheduling()

if __name__ == '__main__':
    uvicorn.run("main:app", port=8000)
