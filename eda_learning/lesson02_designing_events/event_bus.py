"""An in-memory pub/sub bus that carries JSON text, closer to a real broker than Lesson 01's.

- Each subscriber gets its own queue and thread, like a separate Kafka consumer group:
  a slow subscriber doesn't hold up the others.
- Subscribers pick events by their CloudEvents `type`.
- Every published event is appended to a log file, like a topic with retention.
"""
import json
import queue
import threading


class Bus:
    def __init__(self, log_path):
        self._subscribers = []  # (types, queue)
        self._log_path = log_path
        open(log_path, "w").close()  # start with an empty log

    def subscribe(self, name, types, handler):
        q = queue.Queue()
        self._subscribers.append((set(types), q))
        threading.Thread(target=self._run, args=(name, q, handler), daemon=True).start()

    def publish(self, event_json):
        with open(self._log_path, "a") as f:
            f.write(event_json + "\n")
        type_ = json.loads(event_json)["type"]
        for types, q in self._subscribers:
            if type_ in types:
                q.put(event_json)

    def _run(self, name, q, handler):
        while True:
            event_json = q.get()
            try:
                handler(event_json)
            except Exception as e:
                print(f"    [{name}] FAILED: {e!r}")
            q.task_done()

    def close(self):
        """Block until every subscriber has handled everything."""
        for _, q in self._subscribers:
            q.join()
