import click
import importlib


@click.command()
@click.option('--generator_name', help='Generator Name.')
def main(generator_name):
    generator_module = importlib.import_module(f'generators.{generator_name}')
    generator_module.main()


if __name__ == "__main__":
    main()

