run scrape_url.py 

scrape_url.py  sends get request from the api of cargurus 

the scraping for the given make stops when it finds the previous listing id in the result

and the resulting data is obtained in form of csv is uploaded to s3.