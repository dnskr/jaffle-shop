import os
from typing import Tuple

import dlt
from dlt.common.pipeline import StepInfo, ExtractInfo, NormalizeInfo, LoadInfo
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=HeaderLinkPaginator()
)


@dlt.resource
def customers():
    for page in client.paginate("customers"):
        yield page


@dlt.resource
def orders():
    for page in client.paginate("orders"):
        yield page


@dlt.resource
def products():
    for page in client.paginate("products"):
        yield page


def run_naive() -> Tuple[ExtractInfo, NormalizeInfo, LoadInfo]:
    """
    Naive pipeline version without optimizations
    """
    pipeline = dlt.pipeline(
        pipeline_name="naive_jaffle",
        destination="duckdb",
        dataset_name="jaffle",
        dev_mode=True
    )
    return (
        pipeline.extract([customers, orders, products]),
        pipeline.normalize(),
        pipeline.load()
    )


def run_optimized(parallelized=False, workers: int = None, buffer: int = None, max_items: int = None) -> Tuple[ExtractInfo, NormalizeInfo, LoadInfo]:
    """
    Optimized pipeline version with tunable values
    """

    @dlt.source(parallelized=parallelized)
    def jaffle_source():
        return customers, orders, products

    if workers:
        os.environ['EXTRACT__WORKERS'] = str(workers)
        os.environ['NORMALIZE__WORKERS'] = str(workers)
        os.environ['LOAD__WORKERS'] = str(workers)
    else:
        os.environ.pop('EXTRACT__WORKERS', None)
        os.environ.pop('NORMALIZE__WORKERS', None)
        os.environ.pop('LOAD__WORKERS', None)

    if buffer:
        os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = str(buffer)
        os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(buffer)
    else:
        os.environ.pop('DATA_WRITER__BUFFER_MAX_ITEMS', None)
        os.environ.pop('NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS', None)

    if max_items:
        os.environ['EXTRACT__DATA_WRITER__FILE_MAX_ITEMS'] = str(max_items)
        os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = str(max_items)
    else:
        os.environ.pop('EXTRACT__DATA_WRITER__FILE_MAX_ITEMS', None)
        os.environ.pop('NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS', None)

    pipeline = dlt.pipeline(
        pipeline_name=f"optimized_jaffle_{parallelized}_{workers}_{buffer}_{max_items}",
        destination="duckdb",
        dataset_name="jaffle"
    )
    return (
        pipeline.extract(jaffle_source()),
        pipeline.normalize(),
        pipeline.load()
    )


def step_duration(step_info: StepInfo):
    return step_info.finished_at - step_info.started_at


def info_duration(info: Tuple[ExtractInfo, NormalizeInfo, LoadInfo]):
    extract = step_duration(info[0]).total_seconds()
    normalize = step_duration(info[1]).total_seconds()
    load = step_duration(info[2]).total_seconds()
    return {
        "extract": extract,
        "normalize": normalize,
        "load": load,
        "total": extract + normalize + load
    }

# result_info = run_naive()
# print("Naive pipeline version without optimizations")
# print(info_duration(result_info))
# print()
#
# cpu_count = os.cpu_count() # number of workers reflects number of CPUs
# param_sets = [
#     {},
#     {"parallelized": True},
#     {"parallelized": True, "workers": int(cpu_count / 2)},
#     {"parallelized": True, "workers": cpu_count},
#     {"parallelized": True, "workers": cpu_count, "buffer": 10_000},
#     {"parallelized": True, "workers": cpu_count, "buffer": 50_000},
#     {"parallelized": True, "workers": cpu_count, "buffer": 100_000},
#     {"parallelized": True, "workers": cpu_count, "buffer": 100_000, "max_items": 10_000},
#     {"parallelized": True, "workers": cpu_count, "buffer": 100_000, "max_items": 50_000},
#     {"parallelized": True, "workers": cpu_count, "buffer": 100_000, "max_items": 100_000}
# ]
# for params in param_sets:
#     result_info = run_optimized(**params)
#     print(f"Grouped resources + {params}")
#     print(f"Duration: {info_duration(result_info)}")
#     print()
