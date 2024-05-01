import time
from typing import Optional, Literal

import click

from src.application.data_processor import ProcessorProps, DataProcessor
from src.infrastructure.aligner import Aligner
from src.infrastructure.mp_worker import MPWorkerSync, MPWorkerPool, MPWorkerQueue
from src.infrastructure.reader import Reader


# logger = AppLogger()

ProcessType = Literal['sync', 'thread_pool']
ProcessTypeDefault = 'sync'

mp_worker_to_process_type = {
    'sync': MPWorkerSync(),
    'pool': MPWorkerPool(),
    'queue': MPWorkerQueue(),
}


@click.group()
def cli():
    pass


@click.command()
@click.argument('file', type=click.File('r'))
@click.option('--process_type', type=str, default=ProcessTypeDefault, help='Process type', show_default=True)
@click.option('--output', type=str, help='Output file')
@click.option('--verbose', is_flag=True, help='Verbose output')
@click.option('--limit', help='Limit the number of sequences to process', default=None)
def process(file: click.File, process_type: ProcessType, output: click.File, verbose: bool, limit: Optional[int]):
    if not output:
        output = 'output.fasta'

    file_name = file.name
    click.echo(f'File: {file_name}')
    click.echo(f'Process type: {process_type}')
    click.echo(f'Output: {output}')
    click.echo(f'Verbose: {verbose}')
    click.echo('Processing...')

    mp_worker = mp_worker_to_process_type[process_type]
    props = ProcessorProps(
        limit=limit,
    )
    file_reader = Reader(file_name)
    aligner = Aligner()
    processor_instance = DataProcessor(props, mp_worker, file_reader, aligner)

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
