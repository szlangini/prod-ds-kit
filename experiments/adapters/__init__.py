from .base import EngineAdapter
from .duckdb import DuckDBAdapter
from .postgres import PostgresAdapter
from .cedardb import CedarDBAdapter


ADAPTERS = {
    "duckdb": DuckDBAdapter,
    "postgres": PostgresAdapter,
    "cedardb": CedarDBAdapter,
}
