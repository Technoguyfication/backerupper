from typing import Any, Callable

class EventEmitter():

    def __init__(self) -> None:
        self.__events: dict[str, list[Callable[..., Any]]] = {}
    
    def on(self, event: str, callback: Callable):
        if not self.__events[event]:
            self.__events[event] = [callback]
        else:
            self.__events[event].append(callback)
    
    def off(self, event: str, callback: Callable):
        if not self.__events[event]:
            return
        
        self.__events[event].remove(callback)
    
    def clear_listeners(self, event: str | None = None):
        """Clear all listeners for an event (if specified). If not event is specified, clears ALL listeners."""
        if not event:
            self.__events = {}
        elif self.__events[event]:
            self.__events.pop(event)
    
    def _emit(self, event):
        if not self.__events[event]:
            return
        
        for callback in self.__events[event]:
            try:
                callback()
            except:
                # nothing we can do here. print a message maybe? idc
                pass
    