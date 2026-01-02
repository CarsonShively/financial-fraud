import streamlit as st
import pandas as pd

import logging
from time import perf_counter

from financial_fraud.serving.startup import (
    load_champion_model,
    connect_feature_store,
    download_online_logs,
)
from financial_fraud.serving.stream import TxnStream
from financial_fraud.serving.score import score_transaction
from financial_fraud.serving.explain import make_top_factor_explainer
from financial_fraud.logging_utils import setup_logging
from financial_fraud.serving.output import (
    format_output,
    make_log_row_from_out,
    append_log_row_local,
    LOG_COLS,
)

log = logging.getLogger(__name__)


def _ms(t0: float) -> int:
    return int((perf_counter() - t0) * 1000)


def _log_step(name: str, t0: float, **kv) -> None:
    extras = " ".join(f"{k}={v}" for k, v in kv.items() if v is not None)
    if extras:
        log.info("%s %dms %s", name, _ms(t0), extras)
    else:
        log.info("%s %dms", name, _ms(t0))


@st.cache_resource
def get_model_and_ptr():
    return load_champion_model()


@st.cache_resource
def get_redis():
    return connect_feature_store()


@st.cache_data
def get_logs_path():
    return download_online_logs()


@st.cache_resource
def get_explainer_bundle(_model, model_run_id: str):
    return make_top_factor_explainer(_model)


@st.cache_resource
def init_logging():
    setup_logging()
    return True


def get_stream() -> TxnStream:
    if "txn_stream" not in st.session_state:
        t0 = perf_counter()
        logs_path = get_logs_path()
        _log_step("get_logs_path", t0)

        t0 = perf_counter()
        st.session_state.txn_stream = TxnStream(
            parquet_path=logs_path,
            start_step=None,
            batch_size=2048,
        )
        _log_step("stream_init", t0, batch_size=2048)
    return st.session_state.txn_stream


def main():
    t0 = perf_counter()
    init_logging()
    _log_step("init_logging", t0)

    st.title("Fraud Demo")

    st.session_state.setdefault("log_rows", [])
    st.session_state.setdefault("last_out", None)
    st.session_state.setdefault("last_tx", None)

    t0 = perf_counter()
    model, champ = get_model_and_ptr()
    _log_step("load_model", t0, champ_run_id=champ.get("run_id", "unknown"))

    t0 = perf_counter()
    r, cfg = get_redis()
    _log_step("connect_redis", t0, host=getattr(cfg, "host", None), db=getattr(cfg, "db", None))

    t0 = perf_counter()
    current = r.get(cfg.current_pointer_key)
    if current is None:
        raise RuntimeError(f"Missing CURRENT pointer: {cfg.current_pointer_key}")
    run_prefix = current.decode() if isinstance(current, (bytes, bytearray)) else str(current)
    _log_step("read_current_ptr", t0, run_prefix=run_prefix)

    t0 = perf_counter()
    stream = get_stream()
    _log_step("get_stream", t0)

    def handle_one_event():
        t_evt = perf_counter()

        t0 = perf_counter()
        tx = stream.next_one()
        _log_step("stream_next_one", t0, empty=(tx is None))
        st.session_state.last_tx = tx

        if tx is None:
            st.session_state.last_out = None
            _log_step("event_done", t_evt, skipped="eof_or_none")
            return

        champ_run_id = str(champ.get("run_id", "unknown"))

        t0 = perf_counter()
        explainer_bundle = get_explainer_bundle(model, champ_run_id)
        _log_step("get_explainer", t0, champ_run_id=champ_run_id)

        t0 = perf_counter()
        result = score_transaction(
            tx=tx,
            r=r,
            cfg=cfg,
            model=model,
            explainer_bundle=explainer_bundle,
        )
        _log_step("score_transaction", t0, ok=(result is not None))

        if result is None:
            st.session_state.last_out = {
                "skipped": True,
                "reason": "invalid_entity",
                "message": "Missing/invalid name_orig or name_dest",
            }
            _log_step("event_done", t_evt, skipped="invalid_entity")
            return

        t0 = perf_counter()
        out = format_output(result, threshold=0.10)
        st.session_state.last_out = out
        _log_step("format_output", t0, fraud=(out.get("fraud") if isinstance(out, dict) else None))

        t0 = perf_counter()
        row = make_log_row_from_out(out)
        st.session_state["log_rows"] = append_log_row_local(
            st.session_state["log_rows"],
            row,
            max_len=200,
        )
        _log_step("append_log", t0, n=len(st.session_state["log_rows"]))

        _log_step("event_done", t_evt)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Transaction"):
            handle_one_event()
            
        out = st.session_state.get("last_out")

        if isinstance(out, dict) and out.get("skipped"):
            st.warning(out.get("message", "Skipped transaction"))
        elif isinstance(out, (dict, list)) and out:
            st.json(out)
        else:
            st.info("No transaction yet — click **Transaction** to simulate incoming event.")

    with col2:
        if st.button("Reset stream"):
            t0 = perf_counter()
            stream.reset()
            st.session_state.last_tx = None
            st.session_state.last_out = None
            st.session_state["log_rows"] = []
            _log_step("reset_stream", t0)

    df = pd.DataFrame(st.session_state["log_rows"], columns=LOG_COLS)
    st.dataframe(df.iloc[::-1], width="stretch")


if __name__ == "__main__":
    main()
