#from airflow.models import BaseOperator
#from airflow.utils.decorators import apply_defaults
from pywebcopy import save_webpage

import datetime
import os


class ScrapData:
    '''
    This class downloads html files and stores them to a folder

    inputs
    ------
    start_date  :   start date of stock price data to be scraped
    end_date    :   end date of stock price data to be downloaded
    path        :   path where downloaded html files are to be stored
    '''
    def __init__(self, start_date, end_date, path):
        self.start_date = start_date
        self.end_date = end_date
        self.path = path

    def define_date_range(self):
        '''
        Creates a date list for the dates between the start date and end date
        '''
        start_date = datetime.datetime.strptime(self.start_date, '%Y%m%d')
        end_date = datetime.datetime.strptime(self.end_date, '%Y%m%d')

        delta = end_date - start_date
        
        # create a list of dates for which data is to be downloaded from the website link
        dates = []
        for i in range(delta.days + 1):
            dt = start_date + datetime.timedelta(days=i)
            dates.append(str(dt.strftime("%Y%m%d")))
        return dates

    def download_html(self):
        '''
        Downloads html data for the dates in the date range list created by the function above
        '''
        for date in self.define_date_range():
            try:
                save_webpage(
                    url=os.path.join('https://live.mystocks.co.ke/price_list/',date),
                    project_folder=self.path)
            except:
                pass

    if __name__ == "__main__":
        self.download_html(self)
