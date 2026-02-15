from typing import Generic, TypeVar

T = TypeVar("T")

class BaseObjectProxy(Generic[T]):
    __wrapped__: T
    def __init__(self, wrapped: T) -> None: ...

class ObjectProxy(BaseObjectProxy[T]):
    def __init__(self, wrapped: T) -> None: ...
