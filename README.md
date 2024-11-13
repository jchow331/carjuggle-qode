# Carjuggle Scrapers

This repository contains the codebase for all scrapers and their respective cleaning scripts used for CarJuggle


## Getting started for Scrapers

* Clone this repository and create & checkout to your own branch from the `dev` branch

* Create a copy of the `.env.example` file and rename it as `.env` file and add all necessary values

* Create a new directory `logs` in the project root folder. This is where the log files will be stored

* To add a new scraper, create a new folder with the website's name and add it inside the `scrapers` directory

* Add `__init__.py` file inside this newly created directory to treat it as a module (this file can be left blank)

* Add `results` and `trackers` folders:
    * `results` folder to save all the output files(csv, json, etc)
    * `trackers` folder(optional) to save any tracking files(txt, json, etc)

* If your scraper makes use of database, create a new file `models.py` and put the database related codebase inside this file

* Add all the necessary codebase `.py` files inside this newly created folder. If possible, follow the given filenames as a convention:
    * `scrape_url.py` - scraper bot that contains the codebase for scraping out new vehicle URLs and adding them to a file or dB
    * `scrape_detail.py` - scraper bot that contains the codebase for scraping out vehicle details for each newly scraped URLs and adding them to a file or dB
    * `update_url.py` - updater bot that contains the codebase for checking new updates for existing vehicle URLs and updating the URL records in the existing file or dB if any
    * `update_detail.py` updater bot that contains the codebase for checking new updates for existing vehicle details and updating the detail records in the existing file or dB if any
    
        
        > **Important Note:** For `.py` filenames and folder names, use **all lowercase characters separated by underscore('_')**. Lets strictly follow this naming convention for `.py` files and folder names

* Also, add a `README.md` and `requirements.txt` file:
    * `README.md` file contains a short documentation for that particular scraper
    * `requirements.txt` file contains all the required third-party packages to run that particular scraper (don't include unnecessary packages not used for that particular scraper)

* All the script `.py` files needs to have some imports and global variables with their required values and also a `main` function decorated with the `handle_exceptions` decorator for monitoring and email logging purposes as shown below
    ~~~
    # Necessary imports for logging
    import logging
    import os
    from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
    from base.logging_decorator import handle_exceptions

    # Set global variables for logger and emails (change this according to your needs)
    logger_name = 'vinn_auto_scraper_bots_canada'
    email_subject = 'Vinn Auto URL Scraper Bot Alert'
    email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net']
    logger = logging.getLogger(__name__)
    scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed


    @handle_exceptions(logger_name, email_subject, email_toaddrs)
    def main():
        # call your functions or your class object methods here
        
    ~~~

* Update the `.env.example` file with required environment variables if your scripts uses any

* The `base` module folder contains the core utilities/functions that can be re-used by all the scrapers. So, if you create a new utility function that you think can be re-used in other scrapers as well, put it inside the `base` module folder


## Getting started for Pre-processors (cleaning scripts)

* Clone this repository and create & checkout to your own branch from the `dev` branch

* Create a copy of the `.env.example` file and rename it as `.env` file and add all necessary values

* Create a new directory `logs` in the project root folder. This is where the log files will be stored

* To add a new preprocessor, create a new folder with the website's name and add it inside the `preprocessors` directory

* Add `__init__.py` file inside this newly created directory to treat it as a module (this file can be left blank)

* Add the necessary codebase `.py` files inside this newly created folder with the filename as clean_{folder_name}.py for the file containing the main() function: 

        > **Important Note:** For `.py` filenames and folder names, use **all lowercase characters separated by underscore('_')**. Lets strictly follow this naming convention for `.py` files and folder names

* The `requirements.txt` file contains all the required third-party packages to run the cleaning scripts

* All the script `.py` files needs to have some imports and global variables with their required values and also a `main` function decorated with the `handle_exceptions` decorator for monitoring and email logging purposes as shown below
    ~~~
    # Necessary imports for logging
    import logging
    import os
    from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
    from base.logging_decorator import handle_exceptions

    # Set global variables for logger and emails (change this according to your needs)
    logger_name = 'vinn_auto_cleaning_canada'
    email_subject = 'Vinn Auto Cleaning Bot Alert'
    email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net']
    logger = logging.getLogger(__name__)


    @handle_exceptions(logger_name, email_subject, email_toaddrs)
    def main():
        # call your functions or your class object methods here
        
    ~~~

* Update the `.env.example` file with required environment variables if your scripts uses any

* The `base` module folder contains the core utilities/functions that can be re-used by all the scrapers. So, if you create a new utility function that you think can be re-used in other cleaning scripts as well, put it inside the `base` module folder


## Strict Guidelines

* Please try to follow the basic **PEP8 guidelines** (like local variables should be lowercase, 2 blank lines after a class or function, etc.)

* For `.py` filenames and folder names, use **all lowercase characters separated by underscore('_')**

* If your script uses any sort of `print()` statement to print out output in the console, replace it with `logger.info()` instead
    ~~~
    num = 5
    # replace this print
    print("Number is ", num)
    # with this
    logger.info(f"Number is {num}")
    ~~~

* If you are handling a general level exception like `Exception` in your program just to display something rather than actually handling that exception, then do a `raise` at the end
    ~~~
    def add(num1, num2):
        try:
            result = num1 + num2
            return result
        except TypeError:
            result = int(num1) + int(num2)
            return result
        except Exception as e:  # in such a Exception where you just display the error instead of handling it
            logger.info(e)  # or print(e)
            raise   # just raise it again
    ~~~

* If there is any portion of the code (even in the import section) that is not being used right now, but you think might be useful later, then please comment out such imports or code blocks

* Please use meaningful variable names instead of just some random alphabets like 'a' or 'b'

* Write reusable and well-documented code

* Please make sure that you run your script using the following command in the "Usage" section and confirm everything is working fine on your end before pushing to remote GitLab repo


## Usage Scrapers

To run your scraper scripts, use the following command:
~~~
python crawl.py --scraper_name <folder_name> --filename <filename>
~~~

Example Use Case (for vinn_auto):

~~~
python crawl.py --scraper_name vinn_auto --filename scrape_url
~~~

## Usage Cleaning Preprocessors

To run your cleaning preprocessors scripts, use the following command:
~~~
python manual_clean.py --cleaner_name <folder_name>
~~~

Example Use Case (for vinn_auto):

~~~
python manual_clean.py --cleaner_name vinn_auto
~~~

OR to run some extra script inside the cleaner bot folder inside preprocessor(manually)
~~~
python manual_clean.py --cleaner_name kbb_ca --filename clean_kbb_ca_market
~~~

## Usage graph generator with csv create

To run your csv generator for graph scripts, use the following command:
~~~
python graph.py --generator_name <file_name>
~~~

Example Use Case(for aggregator):
~~~
python graph.py --generator_name aggregator
~~~

## Contribution

If you want to contribute to this repository, please create a merge request from your own branch to the `dev` branch
