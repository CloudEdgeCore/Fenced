"""
Unified Provider Plugin Protocol for Fenced.
Allows Model, Tool, Memory, Browser, Storage, and Runtime providers
to be developed, discovered, and registered without modifying the Kernel.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import time


class ProviderType(str, Enum):
    MODEL = "model"
    TOOL = "tool"
    MEMORY = "memory"
    BROWSER = "browser"
    STORAGE = "storage"
    RUNTIME = "runtime"


@dataclass
class HealthStatus:
    status: str  # HEALTHY, DEGRADED, UNHEALTHY
    message: str = ""
    timestamp: float = field(default_factory=time.time)
    metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class ResourceRequirements:
    cpu: str = "100m"
    memory: str = "128Mi"
    gpu: str = ""
    storage: str = ""


@dataclass
class ProviderManifest:
    name: str
    version: str
    type: ProviderType
    capabilities: List[str]
    config_schema: Dict[str, Any] = field(default_factory=dict)
    secrets: List[str] = field(default_factory=list)
    resource_requirements: ResourceRequirements = field(default_factory=ResourceRequirements)

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("Provider name is required")
        if not self.version.strip():
            raise ValueError("Provider version is required")
        if not self.capabilities:
            raise ValueError("At least one capability must be declared")


class Provider(ABC):
    """Base interface for all Fenced providers."""

    @abstractmethod
    def manifest(self) -> ProviderManifest:
        pass

    @abstractmethod
    def health(self) -> HealthStatus:
        pass

    def close(self) -> None:
        pass


class ModelProvider(Provider):
    """Abstract interface for LLM completions and streaming."""

    @abstractmethod
    def generate(self, model: str, messages: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        pass

    @abstractmethod
    def stream(self, model: str, messages: List[Dict[str, Any]], on_chunk: Callable[[Dict[str, Any]], None], **kwargs) -> None:
        pass


class ToolProvider(Provider):
    """Abstract interface for capability tools."""

    @abstractmethod
    def list_tools(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def invoke_tool(self, tool_name: str, arguments: Dict[str, Any], execution_id: str = "") -> Dict[str, Any]:
        pass


class MemoryProvider(Provider):
    """Abstract interface for key-value and vector memory."""

    @abstractmethod
    def put(self, namespace: str, key: str, value: Any, embedding: Optional[List[float]] = None) -> None:
        pass

    @abstractmethod
    def get(self, namespace: str, key: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        pass

    @abstractmethod
    def search(self, namespace: str, query_vector: Optional[List[float]] = None, prefix: str = "", limit: int = 10, min_score: float = 0.0) -> List[Dict[str, Any]]:
        pass


class BrowserProvider(Provider):
    """Abstract interface for web browser automation."""

    @abstractmethod
    def navigate(self, url: str, wait_until: str = "load", timeout_seconds: int = 30) -> Dict[str, Any]:
        pass

    @abstractmethod
    def screenshot(self, full_page: bool = True) -> bytes:
        pass

    @abstractmethod
    def click(self, selector: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def evaluate(self, script: str) -> Any:
        pass


class StorageProvider(Provider):
    """Abstract interface for object storage."""

    @abstractmethod
    def put_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_object(self, key: str) -> bytes:
        pass

    @abstractmethod
    def delete_object(self, key: str) -> None:
        pass


class RuntimeProvider(Provider):
    """Abstract interface for isolated execution sandboxes."""

    @abstractmethod
    def start_instance(self, execution_id: str, image_or_artifact: str, environment: Dict[str, str] = None) -> Dict[str, Any]:
        pass

    @abstractmethod
    def stop_instance(self, execution_id: str, grace_seconds: int = 5) -> None:
        pass

    @abstractmethod
    def inspect_instance(self, execution_id: str) -> Dict[str, Any]:
        pass


class ProviderRegistry:
    """Thread-safe dynamic provider registry."""

    def __init__(self):
        self._providers: Dict[ProviderType, Dict[str, Provider]] = {
            t: {} for t in ProviderType
        }

    def register(self, provider: Provider) -> None:
        m = provider.manifest()
        m.validate()
        bucket = self._providers.setdefault(m.type, {})
        if m.name in bucket:
            raise ValueError(f"Provider {m.name} of type {m.type.value} already registered")
        bucket[m.name] = provider

    def get(self, provider_type: ProviderType, name: str) -> Provider:
        bucket = self._providers.get(provider_type, {})
        if name not in bucket:
            raise KeyError(f"Provider {name} of type {provider_type.value} not found")
        return bucket[name]

    def list(self, provider_type: Optional[ProviderType] = None) -> List[ProviderManifest]:
        if provider_type:
            return [p.manifest() for p in self._providers.get(provider_type, {}).values()]
        results = []
        for bucket in self._providers.values():
            for p in bucket.values():
                results.append(p.manifest())
        return results

    def check_all_health(self) -> Dict[str, HealthStatus]:
        reports = {}
        for p_type, bucket in self._providers.items():
            for name, p in bucket.items():
                reports[f"{p_type.value}/{name}"] = p.health()
        return reports


default_registry = ProviderRegistry()
