import streamlit as st
import pandas as pd

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
        logs_path = get_logs_path()
        st.session_state.txn_stream = TxnStream(
            parquet_path=logs_path,
            start_step=None,
            batch_size=2048,
        )
    return st.session_state.txn_stream


def main():
    init_logging()
    st.title("Fraud Demo")

    st.session_state.setdefault("log_rows", [])
    st.session_state.setdefault("last_out", None)
    st.session_state.setdefault("last_tx", None)

    model, champ = get_model_and_ptr()
    r, cfg = get_redis()

    current = r.get(cfg.current_pointer_key)
    if current is None:
        raise RuntimeError(f"Missing CURRENT pointer: {cfg.current_pointer_key}")
    run_prefix = current.decode() if isinstance(current, (bytes, bytearray)) else str(current)

    stream = get_stream()

    def handle_one_event():
        tx = stream.next_one()
        st.session_state.last_tx = tx

        if tx is None:
            st.session_state.last_out = None
            return

        champ_run_id = str(champ.get("run_id", "unknown"))
        explainer_bundle = get_explainer_bundle(model, champ_run_id)

        result = score_transaction(
            tx=tx,
            r=r,
            cfg=cfg,
            run_prefix=run_prefix,
            model=model,
            explainer_bundle=explainer_bundle,
        )

        out = format_output(result, threshold=0.10)
        st.session_state.last_out = out

        row = make_log_row_from_out(out)
        st.session_state["log_rows"] = append_log_row_local(
            st.session_state["log_rows"],
            row,
            max_len=200,
        )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Transaction"):
            handle_one_event()

    with col2:
        if st.button("Reset stream"):
            stream.reset()
            st.session_state.last_tx = None
            st.session_state.last_out = None
            st.session_state["log_rows"] = []

    out = st.session_state.get("last_out")

    if isinstance(out, (dict, list)) and out:
        st.json(out)
    else:
        st.info("No transaction yet — click **Transaction** to simulate incoming event.")


    df = pd.DataFrame(st.session_state["log_rows"], columns=LOG_COLS)
    st.dataframe(df.iloc[::-1], width="stretch")


if __name__ == "__main__":
    main()
