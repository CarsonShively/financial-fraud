import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from financial_fraud.serving.startup import (
    load_champion_model,
    connect_feature_store,
    register_lua_scripts,
)
from financial_fraud.config import ONLINE_TRANSACTIONS, REPO_ID, REVISION, TRANSACTION_LOG
from financial_fraud.io.hf import download_dataset_hf
from financial_fraud.serving.steps.explain import top_factor_explainer
from financial_fraud.stream.stream import TxnStream
from financial_fraud.serving.serve import serve
from financial_fraud.stream.build_log import local_log
from financial_fraud.serving.warm_start import warm_start
from financial_fraud.serving.warm_up_start_step import compute_start_step
import json
import time
from pathlib import Path


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
def get_dataset_path(repo_id: str, filename: str, revision: str | None = None) -> str:
    return download_dataset_hf(repo_id=repo_id, filename=filename, revision=revision)

PROJECT_ROOT = Path(__file__).resolve().parents[1]   # adjust parents[...] as needed
WARM_TIMES_PATH = PROJECT_ROOT / "eta" / "warm_start_eta.json"
WARM_TIMES_PATH.parent.mkdir(parents=True, exist_ok=True)

def load_last_warm_seconds() -> float | None:
    try:
        if not WARM_TIMES_PATH.exists():
            return None
        data = json.loads(WARM_TIMES_PATH.read_text())
        val = data.get("last_seconds")
        return float(val) if val is not None else None
    except Exception:
        return None

def save_last_warm_seconds(seconds: float) -> None:
    try:
        payload = {
            "last_seconds": float(seconds),
            "saved_at_unix": time.time(),
        }
        WARM_TIMES_PATH.write_text(json.dumps(payload, indent=2))
    except Exception:
        pass

def ensure_warm_started(*, r, cfg, lua_shas, warm_parquet_path: str, k: int = 48) -> int:
    """
    Warm start exactly once per Streamlit session (reruns won't repeat it).
    Returns the start_step boundary used for warm start.
    """
    cache_key = ("warm_started", warm_parquet_path, k)
    if st.session_state.get(cache_key):
        return int(st.session_state["warm_start_step"])

    # Clean init for the demo session
    r.flushdb()
    register_lua_scripts(r)  # ok even if lua_shas already computed

    start_step = compute_start_step(str(warm_parquet_path), k=k)
    warm_start(r=r, cfg=cfg, lua_shas=lua_shas, start_step=start_step)

    st.session_state[cache_key] = True
    st.session_state["warm_start_step"] = int(start_step)
    return int(start_step)


def get_stream(parquet_path: str, start_step: int | None, batch_size: int) -> TxnStream:
    # If path/start_step/batch_size changes, rebuild the stream.
    sig = (parquet_path, start_step, batch_size)
    if st.session_state.get("stream_sig") != sig:
        st.session_state["stream_sig"] = sig
        st.session_state["stream"] = TxnStream(
            parquet_path=parquet_path,
            start_step=start_step,
            batch_size=batch_size,
        )
    return st.session_state["stream"]


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
        st.session_state["last_out"] = {"skipped": True, "message": "Transaction failed validation."}
        return

    out, log = result
    st.session_state["last_out"] = out
    st.session_state["log_rows"] = local_log(st.session_state["log_rows"], log, max_len=200)


def main():
    st.title("Fraud Demo")

    st.session_state.setdefault("log_rows", [])
    st.session_state.setdefault("last_out", None)
    st.session_state.setdefault("is_streaming", False)

    # deps
    model, champ_ptr, threshold = get_model_and_ptr()
    run_id = str(champ_ptr.get("run_id", champ_ptr.get("path_in_repo", "unknown")))
    explainer_bundle = get_explainer_bundle(model, run_id)

    r, cfg = get_redis()
    lua_shas = get_lua_shas()

    # warm start dataset + apply warm start once
    warm_parquet_path = get_dataset_path(REPO_ID, TRANSACTION_LOG, revision=REVISION)

    status_box = st.status("Warm start", expanded=True)
    eta_line = st.empty()
    elapsed_line = st.empty()

    try:
        cache_key = ("warm_started", warm_parquet_path, 48)

        if st.session_state.get(cache_key):
            warm_step = int(st.session_state.get("warm_start_step", 0))
            status_box.update(
                label=f"Warm start already complete ✅ (warm_step={warm_step})",
                state="complete",
            )
        else:
            status_box.update(label="Warm start (running)…", state="running")

            last_seconds = load_last_warm_seconds()
            if last_seconds and last_seconds > 0:
                eta_line.caption(f"Estimated time: ~{last_seconds:.1f}s (from last run)")
            else:
                eta_line.caption("Estimated time: unknown (first run)")

            t0 = time.perf_counter()
            with st.spinner("Warming Redis feature store…"):
                warm_step = ensure_warm_started(
                    r=r,
                    cfg=cfg,
                    lua_shas=lua_shas,
                    warm_parquet_path=warm_parquet_path,
                    k=48,
                )
            dt = time.perf_counter() - t0

            save_last_warm_seconds(dt)
            elapsed_line.caption(f"Completed in {dt:.1f}s")
            status_box.update(label="Warm start complete ✅", state="complete")

    except Exception as e:
        status_box.update(label="Warm start failed ❌", state="error")
        st.exception(e)
        st.stop()

    # live stream
    logs_path = get_dataset_path(REPO_ID, ONLINE_TRANSACTIONS, revision=None)
    stream_start_step = warm_step + 1  # start AFTER warmup boundary
    batch_size = 2048
    stream = get_stream(logs_path, start_step=stream_start_step, batch_size=batch_size)

    deps = {
        "model": model,
        "threshold": threshold,
        "r": r,
        "cfg": cfg,
        "lua_shas": lua_shas,
        "explainer_bundle": explainer_bundle,
    }

    # controls (auto-only)
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Stream transactions (1s)"):
            st.session_state["is_streaming"] = True
    with c2:
        if st.button("Pause"):
            st.session_state["is_streaming"] = False

    if st.session_state["is_streaming"]:
        st_autorefresh(interval=1000, key="fraud_tick")  # ✅ 1 second
        handle_transaction(stream=stream, deps=deps)

    out = st.session_state["last_out"]
    if isinstance(out, dict) and out.get("skipped"):
        st.warning(out.get("message", "Skipped transaction"))
    elif isinstance(out, (dict, list)) and out:
        st.json(out)
    else:
        if st.session_state["is_streaming"]:
            st.info("Streaming…")
        else:
            st.info("Click **Start (1s)** to begin streaming.")

    df = pd.DataFrame(st.session_state["log_rows"])
    st.dataframe(df.iloc[::-1], width="stretch")

if __name__ == "__main__":
    main()
