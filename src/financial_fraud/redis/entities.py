from dataclasses import dataclass

@dataclass(frozen=True)
class EntitySpec:
    entity_type: str
    entity_col: str
    table: str

ENTITIES = {
    "dest": EntitySpec(entity_type="dest", entity_col="name_dest", table="gold.dest_buckets_24"),
}