from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generic, Optional, TypeVar

from ..constants.operation_status import OperationStatus

T = TypeVar("T")


@dataclass
class Result(Generic[T]):
    status: OperationStatus
    data: Optional[T] = None
    error: Optional[str] = None
    timestamp: datetime = datetime.now(timezone.utc)

    def is_success(self) -> bool:
        return self.status == OperationStatus.SUCCESS

    def unwrap(self) -> T:
        if not self.is_success or self.data is None:
            raise ValueError(f"Cannot unwrap unsuccessfull result: {self.error}")
        return self.data
