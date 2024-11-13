----- README -----

- kbb_ca.py -
kbb_ca.py gets the urls of the newest listings off of kbb.ca, scrapes the listings and adds to the dataframe, and checks the previous listings. This script requires aws_handler.py to read and write to the s3 bucket, which it does automatically.