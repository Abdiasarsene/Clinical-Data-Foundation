# src/storage/writer_factory.py
from src.storage.lake_writer import LakeWriter
from src.storage.modeling_writer import ModelingWriter
from src.storage.warehouse_writer import WarehouseWriter

class WriterFactory:
    @staticmethod
    def get_writer(layer: str, engine, target: str):

        layer = layer.lower()

        if layer in {"silver", "lake"}:
            return LakeWriter(engine, target)

        if layer == "modeling":
            return ModelingWriter(engine, target)

        if layer in {"warehouse", "postgres"}:
            return WarehouseWriter(engine, target)

        raise ValueError(f"Unknown storage layer: {layer}")