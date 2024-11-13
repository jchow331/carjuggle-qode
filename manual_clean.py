from email.policy import default
import click
import importlib


@click.command()
@click.option('--cleaner_name', help='Cleaner Name.')
@click.option('--filename', help='File Name.', default='')
def main(cleaner_name, filename):
    filename = filename if len(filename)!=0 else f"clean_{cleaner_name}"
    cleaner_module = importlib.import_module(f'preprocessors.{cleaner_name}.{filename}')
    cleaner_module.main()


if __name__ == "__main__":
    main()