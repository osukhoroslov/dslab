from __future__ import annotations
import abc
import json
import sys
from typing import Any, Dict, List, Tuple, Union

JSON = Union[Dict[str, "JSON"], List["JSON"], str, int, float, bool, None]

class Message:
    def __init__(self, message_type: str, data: Dict[str, Any]):
        self.message_type = message_type
        self.data = data

    @property
    def type(self) -> str:
        return self.message_type

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any):
        self.data[key] = value

    def remove(self, key: str):
        self.data.pop(key, None)

    @staticmethod
    def from_inner_data(type: str, data: str) -> Message:
        return Message(type, json.loads(data))


class Context(object):
    def __init__(self, time: float):
        self._time = time
        self._sent_messages: List[Tuple[str, str, str]] = list()
        self._sent_local_messages: List[tuple[str, str]] = list()
        self._timer_actions: List[Tuple[str, float, bool]] = list()

    def send(self, msg: Message, to: str):
        """
        Sends a message to the specified process.
        """
        if not isinstance(to, str):
            raise TypeError(
                'to argument has to be string, not {}'.format(type(to)))
        self._sent_messages.append((msg.type, json.dumps(msg.data), to))

    def send_local(self, msg: Message):
        """
        Sends a _local_ message.
        """
        self._sent_local_messages.append((msg.type, json.dumps(msg.data)))

    def set_timer(self, timer_name: str, delay: float):
        """
        Sets a timer that will trigger on_timer callback after the specified delay.
        If there is an active timer with this name, its delay is overridden.
        """
        if not isinstance(timer_name, str):
            raise TypeError(
                'timer_name argument has to be str, not {}'.format(type(timer_name)))
        if not isinstance(delay, (int, float)):
            raise TypeError(
                'delay argument has to be int or float, not {}'.format(type(delay)))
        if delay < 0:
            raise ValueError('delay argument has to be non-negative')
        self._timer_actions.append((timer_name, delay, False))

    def set_timer_once(self, timer_name: str, delay: float):
        """
        Sets a timer that will trigger on_timer callback after the specified delay.
        If there is an active timer with this name, this call is ignored.
        """
        if not isinstance(timer_name, str):
            raise TypeError(
                'timer_name argument has to be str, not {}'.format(type(timer_name)))
        if not isinstance(delay, (int, float)):
            raise TypeError(
                'delay argument has to be int or float, not {}'.format(type(delay)))
        if delay < 0:
            raise ValueError('delay argument has to be non-negative')
        self._timer_actions.append((timer_name, delay, True))

    def cancel_timer(self, timer_name: str):
        """
        Cancels timer with the specified name.
        """
        if not isinstance(timer_name, str):
            raise TypeError(
                'timer_name argument has to be str, not {}'.format(type(timer_name)))
        self._timer_actions.append((timer_name, -1, False))

    def time(self) -> float:
        """
        Returns the current system time.
        """
        return self._time


class StateMember:
    def __init__(self, t: JSON):
        self.inner = t

    def serialize(self):
        return json.dumps(self.inner, default=StateMember._serialize_obj)

    @staticmethod
    def _serialize_obj(obj):
        res = {}
        if hasattr(obj, 'serialize'):
            res['data'] = obj.serialize()
        else:
            res['data'] = obj.__dict__
        res['_module'] = sys.modules[obj.__module__].__name__
        res['_class'] = str(type(obj).__name__)
        return res

    @staticmethod
    def _deserialize_obj(dct):
        if '_module' in dct:
            cls = getattr(sys.modules[dct['_module']], dct['_class'])
            if hasattr(cls, 'deserialize'):
                res = getattr(cls, 'deserialize')(dct['data'])
                return res
            else:
                return cls(**dct['data'])
        return dct

    @staticmethod
    def deserialize(state):
        return StateMember(json.loads(state, object_hook=StateMember._deserialize_obj))


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

    def get_state(self) -> str:
        """
        This method returns the string representation of process state.
        """
        data = {}
        for name, member in self.__dict__.items():
            if type(member) is StateMember:
                data[name] = member.serialize()
        return json.dumps(data)

    def set_state(self, state_encoded: str):
        """
        This method restores the process state by its string representation.
        """
        data = json.loads(state_encoded)
        for name in self.__dict__:
            self.__dict__[name] = None
        for name, member in data.items():
            self.__dict__[name] = StateMember.deserialize(member)

    def __setattr__(self, name, value):
        if name in self.__dict__ and type(self.__dict__[name]) is StateMember:
            self.__dict__[name].inner = value
        else:
            self.__dict__[name] = value

    def __getattribute__(self, name):
        attr_value = object.__getattribute__(self, name)
        if type(attr_value) is StateMember:
            return attr_value.inner
        else:
            return attr_value
