from typing import Any

import pytest
from pydantic import BaseModel

from lythonic import Result
from lythonic.kit import EdgeMethod


@pytest.mark.debug
def test_edge_method():
    def m1(config: BaseModel) -> BaseModel:
        return config

    edge_method = EdgeMethod(m1)
    assert edge_method.input_types == [BaseModel]
    assert edge_method.ok_output_types == [BaseModel]
    assert edge_method.err_output_type is None

    def m2(config: BaseModel) -> tuple[BaseModel, BaseModel]:
        return config, BaseModel()

    edge_method = EdgeMethod(m2)
    assert edge_method.input_types == [BaseModel]
    assert edge_method.ok_output_types == [BaseModel, BaseModel]
    assert edge_method.err_output_type is None

    def m3(config: BaseModel) -> Result[BaseModel, Any]:
        return Result[BaseModel, Any].Ok(config)

    edge_method = EdgeMethod(m3)
    assert edge_method.input_types == [BaseModel]
    assert edge_method.ok_output_types == [BaseModel]
    assert edge_method.err_output_type is Any
