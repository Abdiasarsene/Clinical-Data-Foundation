# src/storage/writer_factory.py
from storage.silver_writer import LakeWriter
from storage.modeling_writer import ModelingWriter
from storage.warehouse_writer import WarehouseWriter

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