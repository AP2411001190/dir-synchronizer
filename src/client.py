import socket, json, os, sys, queue, threading, hashlib, time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SRC = os.path.abspath(sys.argv[1]); HOST, PORT = "127.0.0.1", 9000
q = queue.Queue()      # blocking queue: no busy-waiting
synced = {}            # path -> last synced hash

class Handler(FileSystemEventHandler):
    def on_created(self, e):
        if not e.is_directory: q.put(("put", e.src_path))
    def on_modified(self, e):
        if not e.is_directory: q.put(("put", e.src_path))
    def on_deleted(self, e):
        if not e.is_directory: q.put(("delete", e.src_path))

def send(op, abspath):
    rel = os.path.relpath(abspath, SRC).replace("\\", "/")
    data = b""
    if op == "put":
        with open(abspath, "rb") as f: data = f.read()
        h = hashlib.sha256(data).hexdigest()
        if synced.get(rel) == h: return          # unchanged, skip
        synced[rel] = h
    else:
        synced.pop(rel, None)
    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(json.dumps({"op": op, "path": rel, "size": len(data)}).encode() + b"\n")
        s.sendall(data); s.recv(10)
    print(op, rel)

def uploader():
    while True:
        op, p = q.get()
        for attempt in range(5):                 # retry on failure
            try:
                time.sleep(0.2); send(op, p); break
            except Exception as ex:
                print("retry", attempt, ex); time.sleep(2 ** attempt)

threading.Thread(target=uploader, daemon=True).start()
obs = Observer(); obs.schedule(Handler(), SRC, recursive=True); obs.start()
for root, _, files in os.walk(SRC):              # initial full sync
    for f in files: q.put(("put", os.path.join(root, f)))
try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    obs.stop()
