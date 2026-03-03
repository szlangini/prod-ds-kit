from .base import EngineAdapter
from .clickhouse import ClickHouseAdapter
from .duckdb import DuckDBAdapter
from .postgres import PostgresAdapter
from .cedardb import CedarDBAdapter


ADAPTERS = {
    "duckdb": DuckDBAdapter,
    "clickhouse": ClickHouseAdapter,
    "postgres": PostgresAdapter,
    "cedardb": CedarDBAdapter,
}
