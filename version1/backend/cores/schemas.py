from pydantic import BaseModel, Field
from cores.config import POLL_INTERVAL


class SourceCreate(BaseModel):
    name: str
    host: str
    port: int
    base_path: str = "kx"
    poll_seconds: int = POLL_INTERVAL
    enabled: bool = True
    monitor_keys: list[str] = Field(default_factory=list)
    headers: dict[str, str] = Field(default_factory=dict)
    similarity_threshold: float = 0.92
    mode: str = "v1"


class SourceUpdate(BaseModel):
    name: str | None = None
    host: str | None = None
    port: int | None = None
    base_path: str | None = None
    poll_seconds: int | None = Field(default=None, ge=5, le=86400)
    enabled: bool | None = None
    monitor_keys: list[str] | None = None
    headers: dict[str, str] | None = None
    similarity_threshold: float | None = None
    mode: str | None = None
