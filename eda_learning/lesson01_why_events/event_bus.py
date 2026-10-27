"""Two tiny in-memory event buses. Not production code: just enough to see the ideas."""
import queue
import threading
from collections import defaultdict


class SyncEventBus:
    """publish() calls every subscribed handler right away, in the publisher's own thread."""

    def __init__(self):
        self._handlers = defaultdict(list)  # event class -> [handler, ...]

    def subscribe(self, event_type, handler):
        self._handlers[event_type].append(handler)

    def publish(self, event):
        for handler in self._handlers[type(event)]:
            self._deliver(handler, event)

    def _deliver(self, handler, event):
        # One broken handler must not break the publisher or the other handlers.
        try:
            handler(event)
        except Exception as e:
            print(f"    [bus] {handler.__name__} FAILED on {type(event).__name__}: {e!r}")

    def close(self):
        pass


class AsyncEventBus(SyncEventBus):
    """publish() only puts the event in an in-memory queue. A background thread delivers it later."""

    def __init__(self):
        super().__init__()
        self._queue = queue.Queue()
        threading.Thread(target=self._run, daemon=True).start()

    def publish(self, event):
        self._queue.put(event)

    def _run(self):
        while True:
            event = self._queue.get()
            for handler in self._handlers[type(event)]:
                self._deliver(handler, event)
            self._queue.task_done()

    def close(self):
        """Block until every queued event has been delivered."""
        self._queue.join()
