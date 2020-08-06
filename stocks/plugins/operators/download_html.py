from pywebcopy import save_webpage
import os

def download_html(stock_date):
    save_webpage(
        url=os.path.join('https://live.mystocks.co.ke/price_list/',stock_date)
        project_folder=r'C:\Users\dmutia\OneDrive - Deloitte (O365D)\Personal\The-Data-Company\raw_html',
    )