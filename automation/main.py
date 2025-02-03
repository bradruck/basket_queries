# ?'s Qubole Audience_Basket and Audience_BasketDetail Queries for Kraft-

# Description -
# This automation runs two different Qubole/Hive queries concurrently.  They are launched using an ActiveBatch schedule
# and upon successful conclusion, they then launch further Active Batch processes. There are two (2) inputs in the
# form of dates give to the automation which are inserted into the Qubole/Hive queries as start and end dates. If both
# the queries are completed successfully, the automation exits with a (0), indicating success. Otherwise, if the
# queries do not finish the automation exits with a (1), indicating failure.
#
# Application Information -
# Required modules:     main.py,
#                       automation_manager.py,
#                       qubole_manager.py'
#                       queries.py,
#                       config.ini
# Deployed Location:    //prd-use1a-pr-34-ci-operations-01/home/bradley.ruck/Projects/kraft_basket_queries/
#
# ActiveBatch Trigger:  where ever ? sees fit
# Source Code:          //gitlab.oracledatacloud.com/odc-operations/Kraft_Basket_Queries/
# LogFile Location:     utilizing ActiveBatch automation logging system
#
# Contact Information -
# Primary Users:        Core Services
# Lead Customer:        
# Lead Developer:       Bradley Ruck (bradley.ruck@oracle.com)
# Date Launched:        September, 2018
# Date Updated:

# main module
# Responsible for reading in the basic configurations settings, creating the log file, and creating and launching
# the Automation Manager (AM-), finally it launches the purge_files method to remove log files that
# are older than a prescribed retention period. A console logger option is offered via keyboard input for development
# purposes when the main.py script is invoked. For production, import main as a module and launch the main function
# as main.main(), which uses 'n' as the default input to the the console logger run option.
#
from datetime import datetime, timedelta
import sys
import os
import configparser
import logging

from automation_manager import AutomationManager


# Define a console logger for development purposes
#
def console_logger():
    # define Handler that writes DEBUG or higher messages to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a simple format for console use
    formatter = logging.Formatter('%(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s')
    console.setFormatter(formatter)
    # add the Handler to the root logger
    logging.getLogger('').addHandler(console)


def main(start_date, end_date, con_opt='n'):
    today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')

    # create a configparser object and open in read mode
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create a dictionary of configuration parameters
    config_params = {
        "qubole_token":         config.get('Qubole', 'saraholden-prod-operations-consumer'),
        "cluster_label":        config.get('Qubole', 'cluster-label'),
        "query_start_date":     start_date,
        "query_end_date":       end_date
    }

    # logfile path to point to the Operations_limited drive on zfs
    purge_days = config.get('LogFile', 'retention_days')
    log_file_path = config.get('LogFile', 'path')
    logfile_name = '{}{}_{}.log'.format(log_file_path, config.get('Project Details', 'app_name'), today_date)
    # insert 'logfile_name' into list below to run a file logger, omit to only have console logger option
    handlers = []

    # check to see if log file already exits for the day to avoid duplicate execution
    if not os.path.isfile(logfile_name):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s: %(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S',
                            handlers=handlers)

        logger = logging.getLogger(__name__)

        # checks for console logger option, default value set to 'n' to not run in production
        if con_opt and con_opt in ['y', 'Y']:
            console_logger()

        logger.info("Process Start - Sara's Queries - {}\n".format(today_date))

        # create AM-SQ object and launch the process manager
        am_sq = AutomationManager(config_params)
        am_sq.process_manager()

        # search logfile directory for old log files to purge
        #am_sq.purge_files(purge_days, log_file_path)


if __name__ == '__main__':
    # prompt user for use of console logging -> for use in development not production
    #ans = input("\nWould you like to enable a console logger for this run?\n Please enter y or n:\t")
    #print()
    main(sys.argv[1], sys.argv[2])
