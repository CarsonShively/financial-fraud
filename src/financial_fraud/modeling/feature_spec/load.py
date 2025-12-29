import json
from importlib.resources import files

def load_feature_spec() -> dict:
    p = files("financial_fraud.modeling.feature_spec").joinpath("feature_spec.json")
    return json.loads(p.read_text(encoding="utf-8"))