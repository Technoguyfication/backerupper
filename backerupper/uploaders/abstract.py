from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

class AbstractUploader(ABC):

    @property
    @abstractmethod
    def supports_streaming_upload(self) -> bool:
        ...

    @abstractmethod
    def create_object(self, key: str, data: bytes) -> ObjectMetadata:
        ...

    @abstractmethod
    def delete_object(self, key: str):
        ...

    @abstractmethod
    def list_objects(self) -> list[ObjectMetadata]:
        ...
    
    @abstractmethod
    def list_incomplete_uploads(self) -> list[IncompleteStreamingUpload]:
        ...
    
    @abstractmethod
    def abort_incomplete_upload(self, upload: IncompleteStreamingUpload):
        ...
    
    @abstractmethod
    def create_streaming_upload(self, key: str) -> AbstractStreamingUpload:
        ...

class AbstractStreamingUpload(ABC):

    def __init__(self,):
        super().__init__()
        self.completed = False

    @abstractmethod
    def upload_chunk(self, data: bytes) -> int:
        """Uploads a chunk of data. Returns the chunk index."""
        if self.completed:
            raise RuntimeError("Cannot upload chunk to completed upload")
    
    @abstractmethod
    def complete_upload(self) -> ObjectMetadata:
        if self.completed:
            raise RuntimeError("Upload already completed!")
        
        self.completed = True

    @abstractmethod
    def abort_upload(self):
        if self.completed:
            raise RuntimeError("Upload already completed!")
        
        self.completed = True

@dataclass
class IncompleteStreamingUpload:
    key: str
    id: str
    created: datetime

@dataclass
class ObjectMetadata:
    key: str
    created: datetime
    size: int
