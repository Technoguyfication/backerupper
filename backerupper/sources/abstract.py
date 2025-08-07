from abc import ABC, abstractmethod

class AbstractSource(ABC):

    def __init__(self, min_chunk_size: int) -> None:
        super().__init__()

        self.min_chunk_size = min_chunk_size

    @abstractmethod
    def start(self):
        ...
    
    @abstractmethod
    def abort(self):
        ...

    @abstractmethod
    def read_data(self) -> bytes | None:
        ...

    @abstractmethod
    def join(self):
        ...
    
    @property
    @abstractmethod
    def name(self) -> str:
        ...
    
    @property
    @abstractmethod
    def completed(self) -> bool:
        ...
