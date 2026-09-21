import threading
import queue
import json

class EventBus:
    def __init__(self):
        self.listeners = []
        self.lock = threading.Lock()

    def subscribe(self):
        q = queue.Queue(maxsize=100)
        with self.lock:
            self.listeners.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.listeners:
                self.listeners.remove(q)

    def emit_sse(self, event_type: str, payload: dict):
        msg = f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"
        with self.lock:
            for q in self.listeners:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass

# Global SSE Publisher
sse_bus = EventBus()