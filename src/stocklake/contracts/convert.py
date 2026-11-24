"""Chuyển schema trung lập sang Spark / pyarrow / Markdown. Import thư viện ngoài trong hàm."""

from __future__ import annotations

from collections.abc import Iterable

from stocklake.contracts.schemas import ALL_SCHEMAS, TableSchema


def to_spark(schema: TableSchema):
    """Trả về pyspark.sql.types.StructType."""
    from pyspark.sql import types as T

    simple = {
        "string": T.StringType(),
        "int": T.IntegerType(),
        "long": T.LongType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
        "array<string>": T.ArrayType(T.StringType()),
    }
    return T.StructType([T.StructField(c.name, simple[c.type], c.nullable) for c in schema.columns])


def to_arrow(schema: TableSchema):
    """Trả về pyarrow.Schema (timestamp UTC, đơn vị micro giống Spark)."""
    import pyarrow as pa

    simple = {
        "string": pa.string(),
        "int": pa.int32(),
        "long": pa.int64(),
        "double": pa.float64(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "array<string>": pa.list_(pa.string()),
    }
    return pa.schema([pa.field(c.name, simple[c.type], c.nullable) for c in schema.columns])


def to_markdown(schemas: Iterable[TableSchema] = ALL_SCHEMAS) -> str:
    """Sinh nội dung docs/data_dictionary.md."""
    lines = [
        "# Từ điển dữ liệu",
        "",
        "Sinh tự động từ `src/stocklake/contracts/schemas.py` bằng `make docs`. Không sửa tay.",
        "",
    ]
    for s in schemas:
        lines += [f"## {s.layer}.{s.name}", ""]
        if s.description:
            lines += [s.description, ""]
        lines.append(f"- Khoá chính: {', '.join(f'`{k}`' for k in s.primary_key)}")
        if s.partition_by:
            parts = ", ".join(
                f"`{p}`" + (" (suy ra)" if p in s.derived_partitions else "")
                for p in s.partition_by
            )
            lines.append(f"- Phân vùng: {parts}")
        lines += [
            "",
            "| Cột | Kiểu | Null | Mô tả |",
            "|---|---|---|---|",
        ]
        for c in s.columns:
            null = "có" if c.nullable else "không"
            lines.append(f"| `{c.name}` | {c.type} | {null} | {c.description} |")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    print(to_markdown())
