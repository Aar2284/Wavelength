import json
from datetime import date, datetime

from kafka.serializer import Deserializer, Serializer

__all__ = [
    "Utf8StringSerializer",
    "JsonSerializer",
    "JsonDeserializer",
]


def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


class Utf8StringSerializer(Serializer):
    def serialize(self, topic, headers, data):
        if data is None:
            return None
        if isinstance(data, (bytes, bytearray, memoryview)):
            return bytes(data)
        return str(data).encode("utf-8")


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        if data is None:
            return None
        return json.dumps(data, default=_json_default).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, headers, data):
        if data is None:
            return None
        if isinstance(data, (bytes, bytearray, memoryview)):
            data = bytes(data).decode("utf-8")
        return json.loads(data)
