# Your First Schema

This tutorial covers schema definition in depth.

## Basic Model Structure

Every database model inherits from `DbModel`:

```python
from pydantic import Field
from lythonic.state import DbModel

class Product(DbModel["Product"]):
    product_id: int = Field(default=-1, description="(PK)")
    name: str
    price: float
    in_stock: bool = True
```

## Primary Keys

Mark a field as primary key by adding `(PK)` at the start of the description:

```python
user_id: int = Field(default=-1, description="(PK) Unique identifier")
```

Use `default=-1` for auto-increment behavior. When you call `save()` on a model
with `pk=-1`, it inserts a new row and updates the field with the generated ID.

## Foreign Keys

Mark foreign keys with `(FK:Table.field)`:

```python
class Order(DbModel["Order"]):
    order_id: int = Field(default=-1, description="(PK)")
    customer_id: int = Field(description="(FK:Customer.customer_id)")
    total: float
```

This generates a `REFERENCES` clause in the DDL.

## Nullable Fields

Use the union type `| None`:

```python
email: str | None = Field(default=None, description="Optional email")
notes: str | None = None  # Also works without Field()
```

## Enum and Literal Constraints

Use `Literal` for constrained string values:

```python
from typing import Literal

class Order(DbModel["Order"]):
    order_id: int = Field(default=-1, description="(PK)")
    status: Literal["pending", "shipped", "delivered"]
```

This generates a CHECK constraint:

```sql
status TEXT NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered'))
```

## Supported Types

| Python Type | SQLite Type | Notes |
|------------|-------------|-------|
| `int`, `bool` | INTEGER | bool stored as 0/1 |
| `float` | REAL | |
| `str` | TEXT | |
| `bytes` | BLOB | |
| `datetime` | TEXT | ISO format |
| `date` | TEXT | ISO format |
| `Path` | TEXT | String path |
| `Enum` | TEXT | Enum value |
| `BaseModel` | TEXT | JSON string |

## Creating the Schema

```python
from lythonic.state import Schema

SCHEMA = Schema([Customer, Order, Product])
SCHEMA.create_schema(Path("shop.db"))
```

Tables are created in the order listed, so put referenced tables first.
