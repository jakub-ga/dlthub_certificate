"""
Jaffle Shop API Pipeline with Performance Optimizations

This pipeline extracts data from the Jaffle Shop API with performance optimizations:
- Chunking (yielding pages instead of individual rows)
- Parallelism (parallelized resources)
- Worker tuning (configurable via environment variables)
"""

from typing import Any, Iterator

import dlt
from dlt.common.typing import TDataItems
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# API Configuration
API_BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"


@dlt.source
def jaffle_shop_source() -> list[Any]:
    """Jaffle Shop API source with all resources."""
    client = RESTClient(
        base_url=API_BASE_URL,
        paginator=PageNumberPaginator(base_page=1, total_path=None),
    )

    @dlt.resource(
        name="customers",
        parallelized=True,
    )
    def customers() -> Iterator[TDataItems]:
        for page in client.paginate("/customers"):
            # Yield entire page as chunk for better performance
            yield page

    @dlt.resource(
        name="orders",
        parallelized=True,
    )
    def orders() -> Iterator[TDataItems]:
        for page in client.paginate("/orders"):
            # Yield entire page as chunk for better performance
            yield page

    @dlt.resource(
        name="products",
        write_disposition="replace",
        parallelized=True,
    )
    def products() -> Iterator[TDataItems]:
        for page in client.paginate("/products"):
            # Yield entire page as chunk for better performance
            yield page

    return [customers, orders, products]


def boosted_pipeline() -> None:
    # Set worker configuration via environment variables
    # os.environ["EXTRACT__WORKERS"] = "4"
    # os.environ["NORMALIZE__WORKERS"] = "4"
    # os.environ["LOAD__WORKERS"] = "4"

    # file rotation
    # os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "10000"

    # buffer
    # os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "20000"

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_boosted",
        destination="duckdb",
        dataset_name="jaffle_shop_dataset_boosted",
    )

    # Run the pipeline
    pipeline.run(jaffle_shop_source())

    print(f"Pipeline trace: {pipeline.last_trace}")


if __name__ == "__main__":
    boosted_pipeline()
