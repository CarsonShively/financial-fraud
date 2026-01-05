REQUIRED = ("step", "amount", "name_dest")

def validate_base(base: dict) -> bool:
    return all(base.get(k) is not None for k in REQUIRED)
