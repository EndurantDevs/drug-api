import json
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import inspect


class JSONOutputMixin:

    RELATIONSHIPS_TO_DICT = False
    EXCLUDE_FIELDS = ()
    EXECUTABLE_FIELDS = {}

    def __iter__(self):
        return self.to_dict().items()

    @staticmethod
    def is_iterable(x):
        """Return whether a value can be iterated."""
        try:
            iter(x)
            return True
        except TypeError:
            return False

    @staticmethod
    def map_anything(x, fn):
        """Recursively apply a function to scalar values inside containers."""
        if isinstance(x, str):
            return fn(x)
        if isinstance(x, dict):
            return {k: JSONOutputMixin.map_anything(v, fn) for k, v in x.items()}
        if JSONOutputMixin.is_iterable(x):
            return [JSONOutputMixin.map_anything(ele, fn) for ele in x]
        return fn(x)

    @staticmethod
    def prepare_for_json(value):
        """Convert common non-JSON scalar values into JSON-safe values."""
        if isinstance(value, (date, datetime)):
            return value.isoformat().split('+')[0] + 'Z'
        if isinstance(value, UUID):
            return str(value)
        return value

    def to_json_dict(self):
        """Return model columns and executable fields as a JSON-safe dict."""
        result_dict = {
            **dict(self._get_column_items()),
            **self._get_executable_fields()
        }
        data = {k: v for k, v in result_dict.items() if k not in self.EXCLUDE_FIELDS}
        return self.map_anything(data, self.prepare_for_json)

    def to_json(self, rel=None):
        """Serialize this model to a JSON string."""
        def extended_encoder(x):
            """Encode datetime and UUID values when dumping JSON."""
            if isinstance(x, datetime):
                return x.isoformat()
            if isinstance(x, UUID):
                return str(x)
            return str(x)
        return json.dumps(self.to_dict(), default=extended_encoder)

    def _get_column_items(self):
        for column in inspect(type(self)).columns:
            value = getattr(self, column.key)
            if (value is None) and (column.default is not None):
                value = column.default.arg
            yield column.key, value

    def _get_executable_fields(self):
        return {key: value(self) for key, value in type(self).EXECUTABLE_FIELDS.items()}
