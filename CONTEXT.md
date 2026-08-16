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
An ordered, duplicate-free sequence of keys naming one axis.
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

## Tabular data

**Frame data**:
Tabular data in a library-agnostic form, independent of pandas, polars, or
pyarrow.
_Avoid_: dataframe, table
