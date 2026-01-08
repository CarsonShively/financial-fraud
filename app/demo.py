import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from financial_fraud.serving.startup import (
    load_champion_model,
    connect_feature_store,
    register_lua_scripts,
)
from financial_fraud.config import ONLINE_TRANSACTIONS, REPO_ID
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.serving.steps.explain import top_factor_explainer
from financial_fraud.stream.stream import TxnStream
from financial_fraud.serving.serve import serve
from financial_fraud.stream.build_log import local_log
from financial_fraud.logging_utils import setup_logging
setup_logging("INFO")


@st.cache_resource
def get_model_and_ptr():
    return load_champion_model()


@st.cache_resource
def get_redis():
    return connect_feature_store()


@st.cache_resource
def get_lua_shas():
    r, _ = get_redis()
    return register_lua_scripts(r)


@st.cache_resource
def get_explainer_bundle(_model, model_run_id: str):
    try:
        return top_factor_explainer(_model)
    except Exception:
        return None


@st.cache_data
def get_logs_path(repo_id: str, filename: str, revision: str | None = None) -> str:
    return download_dataset_hf(repo_id=repo_id, filename=filename, revision=revision)


def get_stream(parquet_path: str, start_step: int | None, batch_size: int) -> TxnStream:
    if "stream" not in st.session_state:
        st.session_state["stream"] = TxnStream(
            parquet_path=parquet_path,
            start_step=start_step,
            batch_size=batch_size,
        )
    return st.session_state["stream"]


def reset_stream(parquet_path: str, start_step: int | None, batch_size: int) -> None:
    r, _ = get_redis()
    r.flushdb()
    register_lua_scripts(r)

    st.session_state["stream"] = TxnStream(
        parquet_path=parquet_path,
        start_step=start_step,
        batch_size=batch_size,
    )
    st.session_state["last_out"] = None
    st.session_state["log_rows"] = []
    st.session_state["is_streaming"] = False



def handle_transaction(*, stream: TxnStream, deps) -> None:
    tx = stream.next_one()
    if tx is None:
        st.session_state["last_out"] = None
        st.session_state["is_streaming"] = False
        return

    result = serve(
        tx,
        r=deps["r"],
        cfg=deps["cfg"],
        model=deps["model"],
        threshold=deps["threshold"],
        explainer_bundle=deps["explainer_bundle"],
        lua_shas=deps["lua_shas"],
    )


    if result is None:
        st.session_state["last_out"] = {
            "skipped": True,
            "message": "Transaction failed validation.",
        }
        return

    out, log = result
    st.session_state["last_out"] = out
    st.session_state["log_rows"] = local_log(st.session_state["log_rows"], log, max_len=200)


def main():
    
    st.title("Fraud Demo")

    st.session_state.setdefault("log_rows", [])
    st.session_state.setdefault("last_out", None)
    st.session_state.setdefault("is_streaming", False)

    model, champ_ptr, threshold = get_model_and_ptr()
    run_id = str(champ_ptr.get("run_id", champ_ptr.get("path_in_repo", "unknown")))
    explainer_bundle = get_explainer_bundle(model, run_id)

    r, cfg = get_redis()
    r.flushdb()
    lua_shas = register_lua_scripts(r)


    logs_path = get_logs_path(REPO_ID, ONLINE_TRANSACTIONS, revision=None)

    start_step = None
    batch_size = 2048
    stream = get_stream(logs_path, start_step=start_step, batch_size=batch_size)

    deps = {
        "model": model,
        "threshold": threshold,
        "r": r,
        "cfg": cfg,
        "lua_shas": lua_shas,
        "explainer_bundle": explainer_bundle,
    }


    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("Transaction", disabled=st.session_state["is_streaming"]):
            handle_transaction(stream=stream, deps=deps)
    with c2:
        if st.button("Start (2s)"):
            st.session_state["is_streaming"] = True
    with c3:
        if st.button("Stop"):
            st.session_state["is_streaming"] = False
    with c4:
        if st.button("Reset stream"):
            reset_stream(logs_path, start_step=start_step, batch_size=batch_size)

    if st.session_state["is_streaming"]:
        st_autorefresh(interval=2000, key="fraud_tick")
        handle_transaction(stream=stream, deps=deps)

    out = st.session_state["last_out"]
    if isinstance(out, dict) and out.get("skipped"):
        st.warning(out.get("message", "Skipped transaction"))
    elif isinstance(out, (dict, list)) and out:
        st.json(out)
    else:
        st.info("No transaction yet — click **Transaction** or **Start (2s)**.")

    df = pd.DataFrame(st.session_state["log_rows"])
    st.dataframe(df.iloc[::-1], use_container_width=True)


if __name__ == "__main__":
    main()
