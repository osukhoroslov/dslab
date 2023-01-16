from __future__ import annotations
import abc
import json
from typing import Any, List, Dict, Tuple


class Message:
    def __init__(self, message_type: str, data: Dict[str, Any]):
        self._type = message_type
        self._data = data

    @property
    def type(self) -> str:
        return self._type

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any):
        self._data[key] = value

    def remove(self, key: str):
        self._data.pop(key, None)

    @staticmethod
    def from_json(message_type: str, json_str: str) -> Message:
        return Message(message_type, json.loads(json_str))


class Context(object):
    def __init__(self, time: float):
        self._time = time
        self._sent_messages: List[Tuple[str, str, str]] = list()
        self._sent_local_messages: List[tuple[str, str]] = list()
        self._timer_actions: List[Tuple[str, float, bool]] = list()

    def send(self, msg: Message, to: str):
        """
        Sends message to specified process
        """
        if not isinstance(to, str):
            raise TypeError('to argument has to be string, not {}'.format(type(to)))
        self._sent_messages.append((msg.type, json.dumps(msg._data), to))

    def send_local(self, msg: Message):
        """
        Sends a local message
        """
        self._sent_local_messages.append((msg.type, json.dumps(msg._data)))

    def set_timer(self, timer_name: str, delay: float):
        """
        Sets timer that will trigger on_timer callback after specified delay. Overrides previous timer with the same name.
        """
        if not isinstance(timer_name, str):
            raise TypeError('timer_name argument has to be str, not {}'.format(type(timer_name)))
        if not isinstance(delay, (int, float)):
            raise TypeError('delay argument has to be int or float, not {}'.format(type(delay)))
        if delay < 0:
            raise ValueError('delay argument has to be non-negative')
        self._timer_actions.append((timer_name, delay, False))

    def set_timer_once(self, timer_name: str, delay: float):
        """
        Sets timer that will trigger on_timer callback after specified delay. Is ignored if there is an active timer with the same name.
        """
        if not isinstance(timer_name, str):
            raise TypeError('timer_name argument has to be str, not {}'.format(type(timer_name)))
        if not isinstance(delay, (int, float)):
            raise TypeError('delay argument has to be int or float, not {}'.format(type(delay)))
        if delay < 0:
            raise ValueError('delay argument has to be non-negative')
        self._timer_actions.append((timer_name, delay, True))

    def cancel_timer(self, timer_name: str):
        """
        Cancels timer with specified name.
        """
        if not isinstance(timer_name, str):
            raise TypeError('timer_name argument has to be str, not {}'.format(type(timer_name)))
        self._timer_actions.append((timer_name, -1, False))

    def time(self) -> float:
        """
        Returns current system time.
        """ 
        return self._time


class Process:
    @abc.abstractmethod
    def on_local_message(self, msg: Message, ctx: Context):
        """
        This method is called when a _local_ message is received.
        """

    @abc.abstractmethod
    def on_message(self, msg: Message, sender: str, ctx: Context):
        """
        This method is called when a message is received.
        """

    @abc.abstractmethod
    def on_timer(self, timer_name: str, ctx: Context):
        """
        This method is called when a timer fires.
        """
