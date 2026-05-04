"""
Alternative key support for DbModel.

`AltKey` encapsulates a table's alternative key metadata: which fields
compose it, which cascade through foreign keys, and the precomputed
JOIN chain for resolution and serialization queries.

Constructed via `AltKey.from_model()`. Returns `None` if the model has
no `(AK)`-annotated fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from lythonic.state import DbModel


class AltKeyField(NamedTuple):
    """One component of a composite alternative key."""

    name: str
    foreign_key: tuple[str, str] | None  # (TableName, field_name) if FK


class JoinStep(NamedTuple):
    """One JOIN in the cascade chain.

    `local_field` on the current table joins to `remote_field` on
    `remote_table` (aliased as `alias`). `leaf_columns` are the AK
    column names on the remote table to SELECT (using the alias).
    """

    local_field: str
    remote_table: str
    remote_field: str
    alias: str
    leaf_columns: list[str]


class AltKey:
    """
    Describes a table's alternative key and handles bidirectional
    resolution between external AK values and internal integer PKs.

    FK fields in the AK cascade to the referenced table's own AK
    recursively. The cascade chain is precomputed as a list of
    `JoinStep`s used to build JOIN queries at runtime.
    """

    model_cls: type[DbModel[Any]]
    fields: list[AltKeyField]
    _join_chain: list[JoinStep]
    _local_ak_columns: list[str]

    def __init__(
        self,
        model_cls: type[DbModel[Any]],
        fields: list[AltKeyField],
    ) -> None:
        self.model_cls = model_cls
        self.fields = fields
        self._join_chain = []
        self._local_ak_columns = [f.name for f in fields if f.foreign_key is None]

    @classmethod
    def from_model(cls, model_cls: type[DbModel[Any]]) -> AltKey | None:
        """Build an AltKey from a model's (AK)-annotated fields, or None."""
        ak_fields: list[AltKeyField] = []
        for fi in model_cls.get_field_infos():
            if fi.alt_key:
                ak_fields.append(AltKeyField(name=fi.name, foreign_key=fi.foreign_key))
        if not ak_fields:
            return None
        return cls(model_cls, ak_fields)
