"""Biểu đồ giá: đường đóng cửa, nến + khối lượng, nến phút trong ngày."""

from datetime import date, timedelta

import data
import pandas as pd
import streamlit as st

try:
    import altair as alt
except ImportError:  # altair là phụ thuộc của streamlit; chỉ phòng hờ
    alt = None

st.set_page_config(page_title="Biểu đồ giá", layout="wide")
st.title("Biểu đồ giá")
st.caption("Lát 1: đọc tạm lớp clean; từ Lát 2 chuyển sang curated.")

UP, DOWN = "#16a34a", "#dc2626"


def candlestick(df: pd.DataFrame):
    """Nến: râu (low-high) + thân (open-close), xanh tăng / đỏ giảm."""
    d = df.copy()
    d["xu_huong"] = (d["close"] >= d["open"]).map({True: "Tăng", False: "Giảm"})
    color = alt.Color(
        "xu_huong:N",
        scale=alt.Scale(domain=["Tăng", "Giảm"], range=[UP, DOWN]),
        legend=None,
    )
    base = alt.Chart(d).encode(x=alt.X("trade_date:T", title="Ngày"), color=color)
    tip = ["trade_date:T", "open:Q", "high:Q", "low:Q", "close:Q", "volume:Q"]
    y = alt.Y("low:Q", scale=alt.Scale(zero=False), title="Giá (VND, đã điều chỉnh)")
    wick = base.mark_rule().encode(y=y, y2="high:Q", tooltip=tip)
    body = base.mark_bar(size=5).encode(y="open:Q", y2="close:Q", tooltip=tip)
    return (wick + body).properties(height=350)


def volume_chart(df: pd.DataFrame):
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("trade_date:T", title="Ngày"),
            y=alt.Y("volume:Q", title="Khối lượng"),
            tooltip=["trade_date:T", "volume:Q"],
        )
        .properties(height=150)
    )


def show_summary(df: pd.DataFrame, date_col: str):
    if df.empty:
        return
    st.caption(
        data.summary_line(len(df), df["symbol"].nunique(), df[date_col].min(), df[date_col].max())
    )


try:
    symbols = data.list_symbols()
except Exception as exc:  # NameNode không với tới, thiếu fsspec, ...
    st.error(f"Không đọc được danh sách mã từ HDFS: {exc}")
    st.stop()

if not symbols:
    st.info(
        "Chưa có dữ liệu clean daily (/data/clean/daily_ohlcv). "
        "Hãy chạy pipeline (`make e2e-sample`) rồi tải lại trang."
    )
    st.stop()

tab_line, tab_candle, tab_intraday = st.tabs(["Đường đóng cửa", "Nến + khối lượng", "Nến phút"])

# ---------------------------------------------------------------- đường
with tab_line:
    c1, c2 = st.columns([2, 1])
    picked = c1.multiselect("Mã cổ phiếu", symbols, default=symbols[:3], key="line_syms")
    today = date.today()
    rng = c2.date_input("Khoảng ngày", value=(today - timedelta(days=365), today), key="line_range")
    if not picked:
        st.info("Chọn ít nhất một mã.")
    elif not (isinstance(rng, tuple) and len(rng) == 2):
        st.info("Chọn đủ ngày bắt đầu và ngày kết thúc.")
    else:
        try:
            df = data.load_daily(tuple(picked), rng[0], rng[1])
        except Exception as exc:
            st.error(f"Không đọc được dữ liệu: {exc}")
            df = None
        if df is not None:
            if df.empty:
                st.warning("Không có dữ liệu trong khoảng đã chọn.")
            else:
                wide = df.pivot(index="trade_date", columns="symbol", values="close")
                st.line_chart(wide, y_label="Giá đóng cửa (VND, đã điều chỉnh)", x_label="Ngày")
                show_summary(df, "trade_date")
                # df xếp theo (symbol, ngày): tail(10) chỉ ra mã cuối; lấy 10 dòng mới nhất mọi mã.
                st.dataframe(
                    df.sort_values(["trade_date", "symbol"], ascending=[False, True]).head(10),
                    hide_index=True,
                    width="stretch",
                )

# ---------------------------------------------------------------- nến
with tab_candle:
    c1, c2, c3 = st.columns([1, 2, 1])
    sym = c1.selectbox("Mã", symbols, key="candle_sym")
    today = date.today()
    rng2 = c2.date_input(
        "Khoảng ngày", value=(today - timedelta(days=365), today), key="candle_range"
    )
    show_ma = c3.checkbox("Hiện MA20", value=True)
    if not (isinstance(rng2, tuple) and len(rng2) == 2):
        st.info("Chọn đủ ngày bắt đầu và ngày kết thúc.")
    else:
        try:
            d1 = data.load_daily((sym,), rng2[0], rng2[1])
        except Exception as exc:
            st.error(f"Không đọc được dữ liệu: {exc}")
            d1 = None
        if d1 is not None:
            if d1.empty:
                st.warning("Không có dữ liệu trong khoảng đã chọn.")
            else:
                closes = d1["close"].tolist()
                d1["ma20"] = pd.Series(data.moving_average(closes, 20), dtype="float64")
                d1["loi_suat_pct"] = pd.Series(data.daily_return_pct(closes), dtype="float64")
                if alt is not None:
                    chart = candlestick(d1)
                    if show_ma:
                        ma = (
                            alt.Chart(d1)
                            .mark_line(color="#2563eb")
                            .encode(
                                x="trade_date:T", y=alt.Y("ma20:Q", scale=alt.Scale(zero=False))
                            )
                        )
                        chart = chart + ma
                    st.altair_chart(chart, width="stretch")
                    st.altair_chart(volume_chart(d1), width="stretch")
                else:  # dự phòng: đường OHLC + khối lượng
                    st.line_chart(d1.set_index("trade_date")[["open", "high", "low", "close"]])
                    st.bar_chart(d1.set_index("trade_date")["volume"])
                show_summary(d1, "trade_date")
                last = d1.iloc[-1]
                m1, m2 = st.columns(2)
                m1.metric("Đóng cửa gần nhất", f"{last['close']:,.0f}")
                if pd.notna(last["loi_suat_pct"]):
                    m2.metric("Lợi suất ngày", f"{last['loi_suat_pct']:.2f}%")
                st.dataframe(
                    d1.tail(10).sort_values("trade_date", ascending=False),
                    hide_index=True,
                    width="stretch",
                )

# ---------------------------------------------------------------- phút
with tab_intraday:
    sym_i = st.selectbox("Mã", symbols, key="intra_sym")
    try:
        di = data.load_intraday(sym_i)
    except Exception as exc:
        st.error(f"Không đọc được nến phút: {exc}")
        di = None
    if di is not None:
        if di.empty:
            st.info("Chưa có dữ liệu nến phút clean (/data/clean/intraday_1m) cho mã này.")
        else:
            days = sorted(di["trade_date"].dt.date.unique(), reverse=True)
            day = st.selectbox("Ngày giao dịch", days, key="intra_day")
            dd = di[di["trade_date"].dt.date == day]
            st.line_chart(
                dd.set_index("bar_ts_vn")["close"],
                y_label="Giá đóng cửa 1 phút (VND)",
                x_label="Giờ Việt Nam",
            )
            st.caption(
                f"{len(dd):,} nến phút trong ngày {day}; "
                f"{dd['bar_ts_vn'].min():%H:%M} - {dd['bar_ts_vn'].max():%H:%M} (giờ Việt Nam)."
            )
            show_summary(di, "bar_ts_vn")
            st.dataframe(
                dd.tail(10).sort_values("bar_ts", ascending=False),
                hide_index=True,
                width="stretch",
            )
