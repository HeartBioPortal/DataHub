# Processing Very Large Datasets

The repository includes a utility for analysing extremely large VCF datasets.
`tools/large_dataset_processor.py` uses [Dask](https://www.dask.org/) to enable
out-of-core processing so you can handle files exceeding 500 GB on modest
hardware.

```bash
pip install -r requirements.txt
python tools/large_dataset_processor.py path/to/your.vcf
```

The script counts variant records using a distributed computation graph. You can
pass glob patterns to process multiple shards in parallel.
