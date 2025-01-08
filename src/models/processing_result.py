from dataclasses import dataclass
from typing import List


@dataclass
class ProcessingResult:
    records_processed: int
    records_failed: int
    error_messages: List[str]
    processing_time: float

    @property
    def is_complete_success(self) -> bool:
        return self.records_failed == 0
