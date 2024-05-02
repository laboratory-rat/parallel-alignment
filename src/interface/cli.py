import time
from typing import Optional, Literal, List

import click

from src.application.data_processor import ProcessorProps, DataProcessor
from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner.simple import AlignerSimple
from src.infrastructure.aligner.numba import AlignerNumba
from src.infrastructure.reader import Reader
from src.infrastructure.worker.celery import WorkerCelery
from src.infrastructure.worker.dask import WorkerDask
from src.infrastructure.worker.pool import WorkerPool
from src.infrastructure.worker.sync import WorkerSync

ProcessType = Literal['sync', 'pool', 'dask']
ProcessTypeDefault = 'sync'

mp_worker_to_process_type = {
    'sync': WorkerSync[List[SequenceData]],
    'pool': WorkerPool[List[SequenceData]],
    'dask': WorkerDask[List[SequenceData]],
    'celery': WorkerCelery[List[SequenceData]],
}


@click.group()
def cli():
    pass


@click.command()
@click.argument('file', type=click.File('r'))
@click.option('--process_type', type=str, default=ProcessTypeDefault, help='Process type', show_default=True)
@click.option('--workers', type=int, default=4, help='Number of workers', show_default=True)
@click.option('--numba', is_flag=True, help='Use numba for alignment', default=False)
@click.option('--output', type=str, help='Output file')
@click.option('--limit', help='Limit the number of sequences to process', default=None)
def process(file: click.File, process_type: ProcessType, workers: int, numba: bool, output: click.File, limit: Optional[int]):
    if not output:
        output = 'output.fasta'

    workers = max(1, workers)
    if process_type not in mp_worker_to_process_type:
        click.echo(f'Invalid process type: {process_type}')
        click.echo(f'Valid process types: {list(mp_worker_to_process_type.keys())}')
        return

    file_name = file.name
    click.echo(f'File: {file_name}')
    click.echo(f'Process type: {process_type}')
    click.echo(f'Workers: {workers}')
    click.echo(f'Numba: {numba}')
    click.echo(f'Output: {output}')
    click.echo('Processing...')

    worker = mp_worker_to_process_type[process_type](workers)
    props = ProcessorProps(
        limit=limit,
    )
    file_reader = Reader(file_name)
    if numba:
        aligner = AlignerNumba()
    else:
        aligner = AlignerSimple()
    processor_instance = DataProcessor(props, worker, file_reader, aligner)
    final_data = processor_instance.process()

    with open(output + '.txt', 'w') as f:
        for data in final_data:
            f.write(f'{" ".join(data.sequence)}\n')

    with open(output, 'w') as f:
        for data in final_data:
            f.write(f'{data}\n')


cli.add_command(process)


cli()
