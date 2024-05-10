import time

from typing import Optional, Literal, List
import matplotlib.pyplot as plt
import numpy as np

import click
from pydantic import BaseModel

from src.application.data_processor import ProcessorProps, DataProcessor, ProcessorResult
from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner.simple import AlignerSimple
from src.infrastructure.aligner.numba import AlignerNumba
from src.infrastructure.reader import Reader
from src.infrastructure.worker.dask import WorkerDask
from src.infrastructure.worker.pool import WorkerPool
from src.infrastructure.worker.sync import WorkerSync

ProcessType = Literal['sync', 'pool', 'dask']
ProcessTypeDefault = 'sync'

mp_worker_to_process_type = {
    'sync': WorkerSync[List[SequenceData]],
    'pool': WorkerPool[List[SequenceData]],
    'dask': WorkerDask[List[SequenceData]],
    # 'celery': WorkerCelery[List[SequenceData]],
}


class BenchmarkSample(BaseModel):
    process_type: str
    limit: int
    workers: int
    numba: bool
    process_time_total: int
    process_time_worker: int


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
@click.option('--io', is_flag=True, default=False, help='Allow worker to communicate with console')
def process(file: click.File, process_type: ProcessType, workers: int, numba: bool, output: click.File, limit: Optional[int], io: bool):
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

    worker = mp_worker_to_process_type[process_type](workers, io)
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
    click.echo(f'Startup time: {final_data.metadata.total_time_ms - final_data.metadata.processing_time_ms}')
    click.echo(f'Processing time: {final_data.metadata.processing_time_ms}')


    with open(output + '.txt', 'w') as f:
        for data in final_data.value:
            f.write(f'{" ".join(data.sequence)}\n')

    with open(output, 'w') as f:
        for data in final_data:
            f.write(f'{data}\n')


def run_combination(file: click.File, process_type: ProcessType, workers: int, numba: bool, limit: Optional[int]) -> ProcessorResult | None:
    workers = max(1, workers)
    if process_type not in mp_worker_to_process_type:
        click.echo(f'Invalid process type: {process_type}')
        click.echo(f'Valid process types: {list(mp_worker_to_process_type.keys())}')
        return None

    file_name = file.name
    click.echo(f'File: {file_name}')
    click.echo(f'Process type: {process_type}')
    click.echo(f'Workers: {workers}')
    click.echo(f'Numba: {numba}')
    click.echo('Processing...')

    worker = mp_worker_to_process_type[process_type](workers, False)
    props = ProcessorProps(
        limit=limit,
    )
    file_reader = Reader(file_name)
    if numba:
        aligner = AlignerNumba()
    else:
        aligner = AlignerSimple()
    processor_instance = DataProcessor(props, worker, file_reader, aligner)
    return processor_instance.process()


@click.command()
@click.argument('file', type=click.File('r'))
def benchmark(file: click.File):
    process_types = mp_worker_to_process_type.keys()
    process_duration = [200, 400]
    workers = [10]
    numba = [False]

    combinations = [
        {'process_type': p_type, 'workers': w, 'numba': n, 'limit': d}
        for p_type in process_types
        for w in workers
        for d in process_duration
        for n in numba
    ]

    results_typed: List[BenchmarkSample] = []
    for combination in combinations:
        combination_result = run_combination(file, **combination)
        results_typed.append(BenchmarkSample(
            process_time_worker=combination_result.metadata.processing_time_ms,
            process_time_total=combination_result.metadata.total_time_ms,
            limit=combination['limit'],
            numba=combination['numba'],
            process_type=combination['process_type'],
            workers=combination['workers'],
        ))

    results_flatted = {}
    for item in results_typed:
        results_flatted[f"{item.process_type} [{item.limit}]"] = item.process_time_total

    plt.bar(results_flatted.keys(), results_flatted.values())
    plt.xlabel('Combination')
    plt.ylabel('Average Execution Time (s)')
    plt.title('Benchmark Results')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.grid(True)
    plt.show()


cli.add_command(benchmark)
cli.add_command(process)


cli()
