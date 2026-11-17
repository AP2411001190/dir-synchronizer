import socket, json, os, threading, hashlib
from concurrent.futures import ThreadPoolExecutor

ROOT = "remote_store"
lock = threading.Lock()          # protects the shared index
index = {}                       # path -> sha256

def recv_line(conn):
    buf = b""
    while not buf.endswith(b"\n"):
        c = conn.recv(1)
        if not c: break
        buf += c
    return buf

def handle(conn):
    with conn:
        hdr = json.loads(recv_line(conn))
        path = os.path.normpath(hdr["path"])
        if path.startswith("..") or os.path.isabs(path):
            conn.sendall(b"ERR\n"); return
        dest = os.path.join(ROOT, path)
        if hdr["op"] == "delete":
            with lock:
                if os.path.exists(dest): os.remove(dest)
                index.pop(path, None)
        elif hdr["op"] == "put":
            data = b""
            while len(data) < hdr["size"]:
                c = conn.recv(min(65536, hdr["size"] - len(data)))
                if not c: break
                data += c
            os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
            with lock:
                with open(dest, "wb") as f: f.write(data)
                index[path] = hashlib.sha256(data).hexdigest()
                print("synced", path)
        conn.sendall(b"OK\n")

if __name__ == "__main__":
    os.makedirs(ROOT, exist_ok=True)
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", 9000)); s.listen()
    pool = ThreadPoolExecutor(max_workers=8)
    print("Server on 9000")
    while True:
        conn, _ = s.accept()
        pool.submit(handle, conn)
