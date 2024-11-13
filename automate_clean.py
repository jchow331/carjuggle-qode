import os
import click
import importlib
import multiprocessing

@click.command()
@click.option('--start', help='Script runs from')
@click.option('--end', help='Script runs to')
def main(start, end):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Getting the direcotry listing only
    cleaners = [d for d in os.listdir(f"{current_dir}/preprocessors/") if os.path.isdir(f"{current_dir}/preprocessors/{d}")]
    if '__pycache__' in cleaners:
        cleaners.remove('__pycache__')
    clean_func =[]
    cleaners.sort()
    cleaners = cleaners[int(start):int(end)]
    #making list of all the cleaners sripts main (module)
    for cleaner_name in cleaners:
        filename = f"clean_{cleaner_name}"
        cleaner_module = importlib.import_module(f'preprocessors.{cleaner_name}.{filename}')
        clean_func.append(cleaner_module.main())

    clean_func_count = len(clean_func)
    total_processor = multiprocessing.cpu_count()

    if total_processor > 2:
        total_processor -= 1

    if clean_func_count < total_processor:
        total_processor = clean_func_count

    for i in range(total_processor):
        vars()[f"process_{i}"] = multiprocessing.Process(args=(total_processor, i, clean_func))

    for i in range(total_processor):
        vars()[f"process_{i}"].start()

    for i in range(total_processor):
        vars()[f"process_{i}"].join()

if __name__ == '__main__':
    main()
