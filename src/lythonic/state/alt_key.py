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

    def build_chain(self, table_map: dict[str, type[DbModel[Any]]]) -> None:
        """Precompute the JOIN chain by walking FK references recursively.

        Must be called after Schema construction provides the table_map.
        """
        self._join_chain = []
        alias_counter = 0

        def walk_fk_fields(
            fk_ak_fields: list[AltKeyField],
        ) -> None:
            nonlocal alias_counter
            for field in fk_ak_fields:
                if field.foreign_key is None:
                    continue
                ref_table_name, ref_field_name = field.foreign_key
                ref_model = table_map[ref_table_name]
                ref_ak = AltKey.from_model(ref_model)
                assert ref_ak is not None

                alias_counter += 1
                alias = f"_j{alias_counter}"

                # Leaf columns: non-FK AK fields on the referenced table
                leaf_cols = [f.name for f in ref_ak.fields if f.foreign_key is None]

                self._join_chain.append(
                    JoinStep(
                        local_field=field.name,
                        remote_table=ref_table_name,
                        remote_field=ref_field_name,
                        alias=alias,
                        leaf_columns=leaf_cols,
                    )
                )

                # Recurse for FK fields in the referenced table's AK
                ref_fk_fields = [f for f in ref_ak.fields if f.foreign_key is not None]
                if ref_fk_fields:
                    walk_fk_fields(ref_fk_fields)

        fk_fields = [f for f in self.fields if f.foreign_key is not None]
        walk_fk_fields(fk_fields)

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
