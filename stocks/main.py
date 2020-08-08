from download_html import ScrapData

download_data = ScrapData(
    start_date = '20200807',
    end_date = '20200807',
    path = r'C:\DAG\udacity_dend_capstone_project\stocks\data\raw_html'
)
download_data.download_html()


