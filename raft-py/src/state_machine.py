from abc import ABC, abstractmethod
import re
from typing import Any, override


class StateMachine(ABC):
    """
    Abstract base class for application state
    """
    @abstractmethod
    def apply(self, command: dict[str, Any]) -> Any:
        """Apply command to state machine and return the result"""
        pass


    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """Return entire state for snapshotting"""
        pass

    @abstractmethod
    def restore_state(self, snapshot: dict[str, Any]) -> None:
        """Restore from snapshot"""
        pass

# Example 1 - KeyValueStateMachine
class KeyValueStateMachine(StateMachine):
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    @override
    def apply(self, command: dict[str, Any]) -> dict[str, Any]:
        op = command.get('op')
        key = command.get('key')
        result = None

        if op == 'SET':
            self.data[key] = command.get('value')
        elif op == 'GET':
            result = self.data.get(key)
        elif op == 'DELETE':
            self.data.pop(key, None)
        else:
            raise ValueError(f'Invalid command operation: {op}')

        command['result'] = result
        return command

    @override
    def get_state(self) -> dict[str, Any]:
        return dict(self.data)

    @override
    def restore_state(self, snapshot: dict[str, Any]) -> None:
        self.data = dict(snapshot)


# Example 2 - CounterStateMachine (distributed counters: rate limiters,
# sequence/ID generators, replicated tallies)
class CounterStateMachine(StateMachine):
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}

    @override
    def apply(self, command: dict[str, Any]) -> dict[str, Any]:
        op = command.get('op')
        name = command.get('name')
        result = None

        if op == 'INCR':
            self.counters[name] = self.counters.get(name, 0) + command.get('by', 1)
            result = self.counters[name]
        elif op == 'DECR':
            self.counters[name] = self.counters.get(name, 0) - command.get('by', 1)
            result = self.counters[name]
        elif op == 'GET':
            result = self.counters.get(name, 0)
        else:
            raise ValueError(f'Invalid command operation: {op}')

        command['result'] = result
        return command

    @override
    def get_state(self) -> dict[str, Any]:
        return dict(self.counters)

    @override
    def restore_state(self, snapshot: dict[str, Any]) -> None:
        self.counters = dict(snapshot)
