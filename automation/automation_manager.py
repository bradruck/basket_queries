# mobile_id_match_manager module
# Module holds the class => MobileIDMatchManager - manages the Mobile ID Matching Process
# Class responsible for overall program management
#
from datetime import datetime, timedelta
import time
import os
import sys
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing_logging import install_mp_handler
import logging

from qubole_manager import QuboleManager
from queries import SarasQueries

today_date = (datetime.now() - timedelta(hours=7)).strftime('%Y%m%d-%H%M%S')


class AutomationManager(object):
    def __init__(self, config_params):
        self.qubole_token = config_params['qubole_token']
        self.cluster_label = config_params['cluster_label']
        self.query_start_date = config_params['query_start_date']
        self.query_end_date = config_params['query_end_date']
        self.queries = ['Kraft_BasketDetail', 'Kraft_Basket']
        self.results_dict = dict()
        self.issues = []
        self.tickets = []
        self.results = False
        self.logger = logging.getLogger(__name__)

    # Manages the overall automation
    #
    def process_manager(self):
        try:
            self.query_concurrency_manager()
        except Exception as e:
            self.logger.error("?'s-queries Concurrency run failed => {}".format(e))
            sys.exit(1)
        else:
            if self.results:
                self.logger.info("")
                self.logger.info("End of ?'s-queries, successful run, exiting program")
                sys.exit(0)
            else:
                self.logger.warning("End of ?'s-queries, un-successful run, exiting program")
                sys.exit(1)

    # Run the qubole query for each of the queries concurrently
    #
    def query_concurrency_manager(self):
        self.logger.info("\n")
        self.logger.info("Beginning the ?'s-queries concurrent processing")
        self.logger.info("\n")

        # activate concurrency logging handler
        install_mp_handler(logger=self.logger)
        # set the logging level of urllib3 to "ERROR" to filter out 'warning level' logging message deluge
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # launches a thread for each of the tickets
        query_result = ThreadPool(processes=len(self.queries))
        try:
            query_result.map(self.query_manager, self.queries)
            query_result.close()
            query_result.join()
        except Exception as e:
            self.logger.error("?'s-queries Concurrency run failed => {}".format(e))
        else:
            self.logger.info("\n")
            self.logger.info("Concluded the ?'s-queries concurrent processing\n")

    # Runs a twice a week match and returns results
    #
    def query_manager(self, query_name):

        query1_result = True
        query2_result = True

        # set the logging level of Qubole to "WARNING" to filter out 'info level' logging message deluge
        logging.getLogger("qds_connection").setLevel(logging.WARNING)

        if query_name == 'Kraft_BasketDetail':
            query = SarasQueries()
            qubole = QuboleManager(query_name, self.qubole_token,
                                   self.cluster_label, query.query1(self.query_start_date, self.query_end_date))
            query1_result = qubole.get_results()

        elif query_name == 'Kraft_Basket':
            query = SarasQueries()
            qubole = QuboleManager(query_name, self.qubole_token,
                                   self.cluster_label, query.query2(self.query_start_date, self.query_end_date))
            query2_result = qubole.get_results()

        if query1_result and query2_result:
            self.results = True
        else:
            self.results = False

    # Checks the log directory for all files and removes those after a specified number of days
    #
    def purge_files(self, purge_days, purge_dir):
        try:
            self.logger.info(
                "\n\t\tRemove {} days old files from the {} directory".format(purge_days, purge_dir))
            now = time.time()
            for file_purge in os.listdir(purge_dir):
                f_obs_path = os.path.join(purge_dir, file_purge)
                if os.stat(f_obs_path).st_mtime < now - int(purge_days) * 86400 and f_obs_path.split(".")[-1] == "log":
                    time_stamp = time.strptime(time.strftime('%Y-%m-%d %H:%M:%S',
                                                             time.localtime(os.stat(f_obs_path).st_mtime)),
                                               '%Y-%m-%d %H:%M:%S')
                    self.logger.info("Removing File [{}] with timestamp [{}]".format(f_obs_path, time_stamp))

                    os.remove(f_obs_path)

        except Exception as e:
            self.logger.error("{}".format(e))
