import os
from typing import Any, Optional

import dlt
from jaffle_shop import customers, orders, products

@dlt.source(parallelized=True)
def jaffle_source():
    return customers, orders, products

def load_jaffle_shop() -> None:
    workers = os.cpu_count()
    os.environ['EXTRACT__WORKERS'] = str(workers)
    os.environ['NORMALIZE__WORKERS'] = str(workers)
    os.environ['LOAD__WORKERS'] = str(workers)

    buffer = 100_000
    os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = str(buffer)
    os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = str(buffer)

    max_items = 10_000
    os.environ['EXTRACT__DATA_WRITER__FILE_MAX_ITEMS'] = str(max_items)
    os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = str(max_items)

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_pipeline",
        destination="duckdb",
        dataset_name="jaffle"
    )
    load_info = pipeline.run(jaffle_source())
    print(load_info)


if __name__ == "__main__":
    load_jaffle_shop()
