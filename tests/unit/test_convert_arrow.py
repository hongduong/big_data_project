import pytest

from stocklake.contracts import ALL_SCHEMAS, get_schema, to_arrow


def test_to_arrow_all():
    pa = pytest.importorskip("pyarrow")
    for s in ALL_SCHEMAS:
        out = to_arrow(s)
        assert out.names == list(s.column_names)
    sch = to_arrow(get_schema("clean", "daily_ohlcv"))
    assert sch.field("dq_flags").type == pa.list_(pa.string())
    assert sch.field("trade_date").type == pa.date32()
