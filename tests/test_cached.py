from __future__ import annotations

from textwrap import dedent


def test_cache_config_from_yaml():
    from lythonic.compose.cached import CacheConfig

    yaml_str = dedent("""
        rules:
          - gref: "json:dumps"
            namespace_path: "util.dumps"
            min_ttl: 0.5
            max_ttl: 2.0
          - gref: "json:loads"
            min_ttl: 0.25
            max_ttl: 1.0
    """).strip()

    import yaml

    data = yaml.safe_load(yaml_str)
    config = CacheConfig.model_validate(data)
    assert len(config.rules) == 2
    assert config.namespace == "lythonic.compose.cached"
    assert config.rules[0].namespace_path == "util.dumps"
    assert config.rules[0].min_ttl == 0.5
    assert config.rules[0].max_ttl == 2.0
    assert config.rules[1].namespace_path is None
    assert str(config.rules[1].gref) == "json:loads"


def test_namespace_nested_access():
    from lythonic.compose.cached import Namespace

    ns = Namespace()
    ns.install("market.fetch_prices", lambda: "ok")
    ns.install("get_data", lambda: "data")

    assert ns.market.fetch_prices() == "ok"  # pyright: ignore
    assert ns.get_data() == "data"  # pyright: ignore


def test_namespace_path_generates_table_name():
    from lythonic.compose.cached import table_name_from_path

    assert table_name_from_path("market.fetch_prices") == "market__fetch_prices"
    assert table_name_from_path("get_data") == "get_data"
    assert table_name_from_path("a.b.c") == "a__b__c"


def test_generate_ddl_single_param():
    from lythonic.compose import Method
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(ticker: str) -> dict[str, float]:  # pyright: ignore[reportUnusedParameter]
        return {}

    ddl = generate_cache_table_ddl("market__fetch_prices", Method(fetch))
    assert "CREATE TABLE IF NOT EXISTS market__fetch_prices" in ddl
    assert "ticker TEXT NOT NULL" in ddl
    assert "value_json TEXT NOT NULL" in ddl
    assert "fetched_at REAL NOT NULL" in ddl
    assert "PRIMARY KEY (ticker)" in ddl


def test_generate_ddl_multiple_params():
    from lythonic.compose import Method
    from lythonic.compose.cached import generate_cache_table_ddl

    def fetch(from_currency: str, to_currency: str) -> dict[str, float]:  # pyright: ignore[reportUnusedParameter]
        return {}

    ddl = generate_cache_table_ddl("get_exchange_rate", Method(fetch))
    assert "PRIMARY KEY (from_currency, to_currency)" in ddl
