import streamlit as st

from financial_fraud.serving.startup import (
    load_champion_model,
    connect_feature_store,
    download_online_logs,
)
from financial_fraud.serving.stream import TxnStream
from financial_fraud.serving.transaction import transaction_to_1row_df
from financial_fraud.serving.explain import make_top_factor_explainer, top_factor_fraud
from financial_fraud.logging_utils import setup_logging


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

    model, champ = get_model_and_ptr()
    r, cfg = get_redis()
    current = r.get(cfg.current_pointer_key)
    if current is None:
        raise RuntimeError(f"Missing CURRENT pointer: {cfg.current_pointer_key}")

    run_prefix = current.decode() if isinstance(current, (bytes, bytearray)) else str(current)
    st.caption(f"FS CURRENT: {run_prefix}")

    st.caption(f"Model run: {champ.get('run_id')}")
    st.caption(f"Redis db: {getattr(cfg, 'db', None)}")

    stream = get_stream()

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Next txn"):
            tx = stream.next_one()
            st.session_state.last_tx = tx

    with col2:
        n = st.number_input("Stream N", min_value=1, max_value=10000, value=100, step=50)
        if st.button("Run N"):
            last = None
            for _ in range(int(n)):
                last = stream.next_one()
                if last is None:
                    break
            st.session_state.last_tx = last

    with col3:
        if st.button("Reset stream"):
            stream.reset()
            st.session_state.last_tx = None

    st.subheader("Cursor")
    st.json(stream.cursor())

    last_tx = st.session_state.get("last_tx")

    st.subheader("Last transaction")
    st.json(last_tx)

    if last_tx:
        X = transaction_to_1row_df(last_tx, r=r, cfg=cfg, live_prefix=run_prefix)

        proba = float(model.predict_proba(X)[0, 1])
        st.metric("Fraud probability", proba)

        spec, pre, names, explainer = get_explainer_bundle(model, champ.get("run_id", "unknown"))

        factor = top_factor_fraud(
            spec=spec,
            pre=pre,
            names=names,
            explainer=explainer,
            X_row=X,
        )
        st.subheader("Top factor")
        st.write(factor)

if __name__ == "__main__":
    main()