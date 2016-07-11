import logging
from threading import Lock, Timer
from .push import Push


class Channel:
    STATES = dict(closed = "closed",
                  errored = "errored",
                  joined = "joined",
                  joining = "joining")
    EVENTS = dict(close = 'phx_close',
                  error = 'phx_error',
                  join = 'phx_join',
                  reply = 'phx_reply',
                  leave = 'phx_leave')

    def __init__(self, topic, params, socket):
        self._lock = Lock()
        self._state = Channel.STATES['closed']
        self._logger = logging.getLogger()
        self._topic = topic
        self._params = params or {}
        self._socket = socket
        self._bindings = []
        self._timeout = self._socket.timeout()
        self._joined_once = False
        self._join_push = Push(self, Channel.EVENTS['join'], self._params, self._timeout)
        self._push_buffer = []

        self._rejoin_timer = Timer(self._socket.reconnect_after_ms(), self._rejoin_until_connected)
        self._join_push.receive("ok", self._joined)
        self._join_push.receive("timeout", self._join_timeout)

        self.on_close(self._closed)
        self.on_error(self._errored)

        self.on(Channel.EVENTS['reply'], lambda payload, ref: self.trigger(self.reply_event_name(ref), payload))

    def join(self, timeout=None):
        with self._lock:
            timeout = timeout or self._timeout
            if self._joined_once:
                raise RuntimeError("'join' can only be called a single time per channel instance")
            else:
                self._joined_once = True
            self._rejoin(timeout)
            return self._join_push

    def on_close(self, cb):
        self.on(Channel.EVENTS['close'], lambda _1, _2: cb())

    def on_error(self, cb):
        self.on(Channel.EVENTS['error'], lambda payload, _: cb(payload))

    def on(self, event, cb):
        self._bindings.append(dict(event=event, callback=cb))

    def off(self, event):
        self._bindings = [b for b in self._bindings if b['event'] != event]

    def is_member(self, topic):
        return self._topic == topic

    def push(self, event, payload, timeout=None):
        if timeout is None:
            timeout = self._timeout
        if not self._joined_once:
            raise RuntimeError("tried to push '{}' to '{}' before joining"
                               .format(event, self._topic))
        push_event = Push(self, event, payload, timeout)
        if self._can_push():
            push_event.send()
        else:
            push_event.start_timeout()
            self._push_buffer.append(push_event)
        return push_event

    def leave(self, timeout=None):
        timeout = timeout or self._timeout

        def on_close():
            self._logger.debug("channel leave {topic}".format(topic=self._topic))
            self.trigger(Channel.EVENTS['close'], "leave")

        push = Push(self, Channel.EVENTS['leave'], {}, timeout)
        push.receive("ok", on_close)
        push.receive("timeout", on_close)
        push.send()

        if not self._can_push():
            push.trigger("ok", {})

        return push

    def socket(self):
        return self._socket

    def topic(self):
        return self._topic

    def _rejoin_until_connected(self):
        with self._lock:
            if self._socket.is_connected():
                self._rejoin()

    def _joined(self, payload):
        with self._lock:
            self._state = Channel.STATES['joined']
            self._rejoin_timer.cancel()
            for push_event in self._push_buffer:
                push_event.send()
            self._push_buffer = []

    def _join_timeout(self):
        with self._lock:
            if not self._is_joining():
                return
            self._logger.debug("channel timeout ({timeout}) on topic {topic}".format(timeout=self._join_push.timeout(), topic=self._topic))
            self._state = Channel.STATES['errored']
            self._rejoin_timer.start()

    def _closed(self):
        with self._lock:
            self._logger.debug("channel close {topic}".format(topic=self._topic))
            self._state = Channel.STATES['closed']
            self._socket.remove(self)

    def _errored(self, reason):
        with self._lock:
            self._logger.debug("channel error {topic}, reason {reason}".format(topic=self._topic, reason=reason))
            self._state = Channel.STATES['errored']
            self._rejoin_timer.start()

    def _can_push(self):
        return self._socket.is_connected() and self._is_joined()

    def _on_message(self, event, payload, ref):
        """
        Overridable message hook.

        Receives all events for specialized message handling before dispatching
        to the channel callbacks.

        Must return the payload, modified or unmodified.
        """
        return payload

    def _rejoin(self, timeout):
        timeout = timeout or self._timeout
        self._state = Channel.STATES['joining']
        self._join_push.resend(timeout)

    def trigger(self, event, payload=None, ref=None):
        self._on_message(event, payload, ref)
        bindings = [b for b in self._bindings if b['event'] == event]
        for binding in bindings:
            binding['callback'](payload, ref)

    def reply_event_name(self, ref):
        return "chan_reply_{ref}".format(ref=ref)

    def _is_closed(self):
        return self._state == Channel.STATES["closed"]

    def _is_errored(self):
        return self._state == Channel.STATES["errored"]

    def _is_joined(self):
        return self._state == Channel.STATES["joined"]

    def _is_joining(self):
        return self._state == Channel.STATES["joining"]

    def _is_leaving(self):
        return self._state == Channel.STATES["leaving"]
