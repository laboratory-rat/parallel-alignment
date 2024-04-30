import time
from typing import Optional

import click

from src.application.data_processor import ProcessorProps, DataProcessor
from src.infrastructure.aligner import Aligner
from src.infrastructure.logger import AppLogger
from src.infrastructure.reader import Reader


# logger = AppLogger()


@click.group()
def cli():
    pass


@click.command()
@click.argument('file', type=click.File('r'))
@click.option('--multicore', is_flag=True, help='Use multicore processing')
@click.option('--cores', help='Number of cores to use', default=None)
@click.option('--output', type=str, help='Output file')
@click.option('--verbose', is_flag=True, help='Verbose output')
@click.option('--limit', help='Limit the number of sequences to process', default=None)
def process(file: click.File, multicore: bool, cores: Optional[int], output: click.File, verbose: bool, limit: Optional[int]):
    if not output:
        output = 'output.fasta'

    file_name = file.name
    click.echo(f'File: {file_name}')
    click.echo(f'Multicore: {multicore}')
    click.echo(f'Cores: {cores}')
    click.echo(f'Output: {output}')
    click.echo(f'Verbose: {verbose}')
    click.echo('Processing...')

    props = ProcessorProps(
        multiprocessing=multicore,
        num_processors=cores or 1,
        limit=limit,
    )
    file_reader = Reader(file_name)
    aligner = Aligner()
    processor_instance = DataProcessor(props, file_reader, aligner)

    start_time = time.time()
    final_data = processor_instance.process()
    end_time = time.time()

    print(f'Processing time: {end_time - start_time}')

    with open(output + '.txt', 'w') as f:
        for data in final_data:
            f.write(f'{" ".join(data.sequence)}\n')

    with open(output, 'w') as f:
        for data in final_data:
            f.write(f'{data}\n')


cli.add_command(process)


cli()
