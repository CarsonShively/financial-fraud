import pandas as pd
from pathlib import Path
from financial_fraud.io.hf import download_dataset_hf, upload_dataset_hf

def main():
    raw = download_dataset_hf(
        repo_id="carson-shively/financial-fraud",
        filename="data/raw/financial_fraud.csv",
        revision="main",
    )

    df = pd.read_csv(raw)

    df = df.drop(columns=["isFlaggedFraud"])
    df = df.sort_values(["step"]).reset_index(drop=True)

    cutoff = int(df["step"].quantile(0.80))

    offline = df[df["step"] <= cutoff].copy()
    online = df[df["step"] > cutoff].copy()

    online = online.drop(columns=["isFraud"])
    
    out_offline = Path("data/bronze/offline.parquet")
    out_online = Path("data/bronze/online.parquet")
    out_offline.parent.mkdir(parents=True, exist_ok=True)

    offline.to_parquet(out_offline, index=False)
    online.to_parquet(out_online, index=False)

    upload_dataset_hf(
        local_path=out_offline,
        repo_id="carson-shively/financial-fraud",
        hf_path="data/bronze/offline.parquet",
        revision="main",
        commit_message="Add bronze offline split",
    )

    upload_dataset_hf(
        local_path=out_online,
        repo_id="carson-shively/financial-fraud",
        hf_path="data/bronze/online.parquet",
        revision="main",
        commit_message="Add bronze online split",
    )

    print("rows:", len(df))
    print("offline rows:", len(offline))
    print("online rows:", len(online))

if __name__ == "__main__":
    main()