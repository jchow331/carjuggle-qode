import click
import importlib


@click.command()
@click.option('--scraper_name', help='Scraper Name.')
@click.option('--filename', help='Scraper Filename.')
def main(scraper_name, filename):
    scraper_module = importlib.import_module(f'scrapers.{scraper_name}.{filename}')
    scraper_module.main()


if __name__ == "__main__":
    main()

