from dataclasses import dataclass
from typing_extensions import Literal

ErrorStage = Literal["validate_structure", "resolve", "plan", "execute"]
ErrorCode = Literal[
    "no_dimensions_or_metrics"
]

@dataclass(frozen=True)
class DataChainError(Exception):
    """This is the error that will be sent to the LLM if any stage fails"""
    stage: ErrorStage
    code: str
    message: str
    hint: str | None = None
    details: dict | None = None
