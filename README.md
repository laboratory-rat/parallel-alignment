# Python parallel test

This is a simple example of parallel processing in python.

## To run single machine parallel processing

```bash
python main.py process {FILE_TO_PROCESS} [--process_type {sync | poll | dask}] [--workers N] [--output {OUTPUT_FILE}] [--limit {LIMIT}]
```

- `FILE_TO_PROCESS`: The file to process
- `--process_type`: The type of processing to use. Default is `sync`
- `--workers`: The number of workers to use. Default is `4`
- `--output`: The output file to write the results to. Default is `output.txt`
- `--limit`: The number of sequences to process. Files are big, no need to process all info from the file. Default
  is `all`
- `--help`: Show help

The best file to process is `data/BM_OTU_raw.fasta` - it contains smaller sequences and is faster to process.

### For sync test

```bash
python main.py process data/BM_OTU_raw.fasta --process_type sync --limit 100
```

### For pool test

```bash
python main.py process data/BM_OTU_raw.fasta --process_type pool --limit 100
```

## Types

### Sync

This is the simplest way to process data. It is a blocking process, meaning that it will wait for the process to finish before moving to the next one.
Will work fine when the processing data is limited.


### Pool

This is a way to process data in parallel using a pool of workers. It is a non-blocking process, meaning that it will not wait for the process to finish before moving to the next one.
This is a good way to process data when the processing data is big and you want to process it on a single machine.


### Dask

Dask is a parallel computing library that integrates with existing Python libraries like NumPy, Pandas, and Scikit-Learn.

### Celery

Celery is a distributed task queue. It is a way to process data in parallel using celery.
It uses a message broker to distribute the tasks to multiple workers. 
It is a non-blocking process, meaning that it will not wait for the process to finish before moving to the next one.
The broker used in this example is `redis` db.
