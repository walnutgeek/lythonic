# Lythonic

Glossary for the terms this project commits to. Definitions only, no
implementation detail.

## Exposure matrices

**Subject**:
The entity that has exposure. The first axis of an exposure matrix.
_Avoid_: row, row key, x

**Target**:
The thing a subject is exposed to. The second axis of an exposure matrix.
_Avoid_: column, column key, y, factor

**Universe**:
An ordered, duplicate-free sequence of keys naming one axis. A matrix may name
both of its axes with the same universe.
_Avoid_: index, axis labels, keyspace

**Exposure**:
The value at one subject-target cell.
_Avoid_: weight, loading, allocation

**Cell fill**:
The value a cell takes when no record exists for it. A property of a matrix, not
of a request to read one.
_Avoid_: default, fill value, missing value

**Default row**:
A target-to-value mapping applied to a subject that is otherwise unmodeled. A
policy of the code building a matrix, not a fact about the matrix.
_Avoid_: default exposures, template

**Frozen axis**:
An axis that rejects keys outside its declared universe. Axes freeze and thaw
independently.
_Avoid_: locked, closed, sealed

**Cast**:
Producing a new matrix over different universes, dropping keys outside them.
_Avoid_: reindex, project, conform

## Symmetric matrices

**Symmetric matrix**:
A square matrix whose two axes are the same universe and whose value depends on
an unordered pair of keys. Carries no claim about definiteness.
_Avoid_: correlation matrix, covariance matrix, gram matrix

**Pair**:
An unordered pair of keys from the universe, addressing one value. Naming the
same key twice addresses the diagonal.
_Avoid_: cell, edge, entry, tuple

**Diagonal**:
The values at self-pairs, one per key in the universe. Always present, never
absent, and never inferred from the off-diagonal values.
_Avoid_: variances, self-correlations, trace

## Tabular data

**Frame data**:
Tabular data in a library-agnostic form, independent of pandas, polars, or
pyarrow.
_Avoid_: dataframe, table

## Conversions

**Facade**:
The surface one optional library gets on a type, reached through a single
attribute named for that library's conventional import alias (`np`, `pd`, `pl`,
`pa`). Class access gives inbound constructors, instance access outbound
conversions.
_Avoid_: adapter, bridge, accessor, namespace
