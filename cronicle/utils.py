import threading

class Lock(threading.Thread):
    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self.lock

    def __exit__(self, exception_type, exception_value, traceback):
        self.lock.release()

class DataEvent():
    def __init__(self):
        self.data = None
        self.lock = Lock()
        self.event = threading.Event()

    def set(self, data):
        with self.lock:
            self.data = data
        self.event.set()

    def clear(self):
        self.event.clear()
        with self.lock:
            self.data = None

    def wait(self):
        self.event.wait()
        with self.lock:
            return self.data

class Flag:
    def __init__(self):
        self.value = False

    def set(self):
        self.value = True

    def __nonzero__(self):
        return self.value

def wait_for_callback(func):
    event = DataEvent()
    def callback(data):
        event.set(data)
    func(callback)

    return event.wait()
