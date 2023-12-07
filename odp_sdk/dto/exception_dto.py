import sys
import traceback
from typing import Type

from cloudpickle import cloudpickle
from pydantic import BaseModel


class ExceptionDto(BaseModel):
    message: str
    exception_cls: Type[Exception]
    stack_trace: str
    filename: str
    lineno: int
    funcname: str

    def __init__(self, **data):
        exception_cls = data.pop("exception_cls", None)
        if isinstance(exception_cls, list):
            data["exception_cls"] = cloudpickle.loads(exception_cls[1])

        super().__init__(**data, exception_cls=exception_cls)

    @classmethod
    def from_exception(cls) -> "ExceptionDto":
        exc_type, exc_value, exc_traceback = sys.exc_info()

        return cls(
            message=str(exc_value),
            exception_cls=exc_type,
            stack_trace=traceback.format_exc(),
            filename=exc_traceback.tb_frame.f_code.co_filename,
            lineno=exc_traceback.tb_lineno,
            funcname=exc_traceback.tb_frame.f_code.co_name,
        )

    def dict(
        self,
        **kwargs,
    ) -> "DictStrAny":
        dct = super().dict(**kwargs)
        dct["exception_cls"] = [self.exception_cls.__name__, cloudpickle.dumps(self.exception_cls)]
        return dct
