from abc import abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Type

from google.protobuf.json_format import MessageToJson

from feast.feature_view_projection import FeatureViewProjection
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDatasetMeta, SavedDatasetSpec
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)


class _StorageRegistry(type):
    classes_by_proto_attr_name: Dict[str, Type["SavedDatasetStorage"]] = {}

    def __new__(cls, name, bases, dct):
        kls = type.__new__(cls, name, bases, dct)
        if dct.get("_proto_attr_name"):
            cls.classes_by_proto_attr_name[dct["_proto_attr_name"]] = kls
        return kls


class SavedDatasetStorage(metaclass=_StorageRegistry):
    _proto_attr_name: str

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> "SavedDatasetStorage":
        proto_attr_name = storage_proto.WhichOneof("kind")
        return _StorageRegistry.classes_by_proto_attr_name[proto_attr_name].from_proto(
            storage_proto
        )

    @abstractmethod
    def to_proto(self) -> SavedDatasetStorageProto:
        pass


class SavedDataset:
    name: str
    features: List[FeatureViewProjection]
    storage: SavedDatasetStorage

    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def __init__(
        self,
        name: str,
        features: List[FeatureViewProjection],
        storage: SavedDatasetStorage,
    ):
        self.name = name
        self.features = features
        self.storage = storage

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash((id(self), self.name))

    def __eq__(self, other):
        if not isinstance(other, SavedDataset):
            raise TypeError(
                "Comparisons should only involve FeatureService class objects."
            )
        if self.name != other.name:
            return False

        if sorted(self.features) != sorted(other.features):
            return False

        return True

    @staticmethod
    def from_proto(saved_dataset_proto: SavedDatasetProto):
        """
        Converts a SavedDatasetProto to a SavedDataset object.

        Args:
            saved_dataset_proto: A protobuf representation of a SavedDataset.
        """
        ds = SavedDataset(
            name=saved_dataset_proto.spec.name,
            features=[],
            storage=SavedDatasetStorage.from_proto(saved_dataset_proto.spec.storage),
        )
        ds.features.extend(
            [
                FeatureViewProjection.from_proto(projection)
                for projection in saved_dataset_proto.spec.features
            ]
        )

        if saved_dataset_proto.meta.HasField("created_timestamp"):
            ds.created_timestamp = (
                saved_dataset_proto.meta.created_timestamp.ToDatetime()
            )
        if saved_dataset_proto.meta.HasField("last_updated_timestamp"):
            ds.last_updated_timestamp = (
                saved_dataset_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return ds

    def to_proto(self) -> SavedDatasetProto:
        """
        Converts a SavedDataset to its protobuf representation.

        Returns:
            A SavedDatasetProto protobuf.
        """
        meta = SavedDatasetMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)

        spec = SavedDatasetSpec(
            name=self.name,
            features=[projection.to_proto() for projection in self.features],
            storage=self.storage.to_proto(),
        )

        feature_service_proto = SavedDatasetProto(spec=spec, meta=meta)
        return feature_service_proto


class SavedDatasetOptions:
    name: str
    storage: SavedDatasetStorage

    def __init__(self, name: str, storage: SavedDatasetStorage):
        self.name = name
        self.storage = storage
