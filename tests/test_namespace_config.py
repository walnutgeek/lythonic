from __future__ import annotations


def test_config_model_callable_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "market:fetch", "gref": "json:dumps"},
            ]
        }
    )
    assert len(config.entries) == 1
    assert config.entries[0].nsref == "market:fetch"
    assert config.entries[0].gref == "json:dumps"
    assert config.entries[0].cache is None
    assert config.entries[0].dag is None


def test_config_model_cached_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "market:fetch",
                    "gref": "json:dumps",
                    "cache": {"min_ttl": 0.5, "max_ttl": 2.0},
                },
            ]
        }
    )
    assert config.entries[0].cache is not None
    assert config.entries[0].cache.min_ttl == 0.5
    assert config.entries[0].cache.max_ttl == 2.0


def test_config_model_dag_entry():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "pipelines:daily",
                    "dag": {
                        "nodes": [
                            {"label": "fetch", "nsref": "market:fetch"},
                            {"label": "compute", "nsref": "analysis:compute", "after": ["fetch"]},
                        ]
                    },
                },
            ]
        }
    )
    dag = config.entries[0].dag
    assert dag is not None
    assert len(dag.nodes) == 2
    assert dag.nodes[1].after == ["fetch"]


def test_config_model_storage():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "storage": {"cache_db": "cache.db", "dag_db": "runs.db"},
            "entries": [],
        }
    )
    assert config.storage.cache_db == "cache.db"
    assert config.storage.dag_db == "runs.db"


def test_config_model_storage_defaults():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate({"entries": []})
    assert config.storage.cache_db is None
    assert config.storage.dag_db is None


def test_config_entry_validation_neither_gref_nor_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate({"entries": [{"nsref": "bad:entry"}]})
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_config_entry_validation_both_gref_and_dag():
    from pydantic import ValidationError

    from lythonic.compose.namespace_config import NamespaceConfig

    try:
        NamespaceConfig.model_validate(
            {
                "entries": [
                    {
                        "nsref": "bad:entry",
                        "gref": "json:dumps",
                        "dag": {"nodes": []},
                    }
                ]
            }
        )
        raise AssertionError("Expected ValidationError")
    except ValidationError:
        pass


def test_namespace_node_metadata():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    def sample(x: int) -> int:
        return x

    ns = Namespace()
    method = Method(sample)
    node = NamespaceNode(method=method, nsref="t:sample", namespace=ns)
    assert node.metadata == {}

    node.metadata["cache"] = {"min_ttl": 0.5, "max_ttl": 2.0}
    assert node.metadata["cache"]["min_ttl"] == 0.5
