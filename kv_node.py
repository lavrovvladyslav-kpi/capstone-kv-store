#!/usr/bin/env python3
import argparse
import bisect
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ConsistentHashRing:
    """Реалізація Consistent Hashing з віртуальними вузлами для логічних шардів."""
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # hash_val -> shard_id
        self.sorted_keys = [] # відсортовані хеші віртуальних вузлів
        self.shards = set() # множина зареєстрованих shard_id

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_shard(self, shard_id: str):
        if shard_id in self.shards:
            return
        self.shards.add(shard_id)
        for i in range(self.virtual_nodes):
            vnode_key = f"{shard_id}#vnode{i}"
            h = self._hash(vnode_key)
            self.ring[h] = shard_id
            bisect.insort(self.sorted_keys, h)

    def get_shard_id(self, partition_key: str):
        if not self.sorted_keys:
            return None, None
        key_hash = self._hash(partition_key)
        idx = bisect.bisect_right(self.sorted_keys, key_hash)
        if idx == len(self.sorted_keys):
            idx = 0
        vnode_hash = self.sorted_keys[idx]
        shard_id = self.ring[vnode_hash]
        return shard_id, key_hash


# глобальний стан вузла
ROLE = "coordinator" # "coordinator" або "shard"
REPLICA_ROLE = "leader" # "leader" або "follower" (для ролі shard)
SHARD_ID = "shard-1" # логічний ідентифікатор шарда
NODE_ID = "coordinator-1" # унікальний ідентифікатор вузла/репліки
DATA_DIR = os.path.expanduser("~")
LEADER_URL = None # для фолловера: URL лідера свого шарда

RING = ConsistentHashRing(virtual_nodes=100)
STATE_LOCK = threading.RLock()

# для координатора: реєстр наборів реплік кожного шарда
# структура: {shard_id: {"leader": str_url, "replicas": {node_id: {"url": str, "role": str}}, "rr_index": int}}
REPLICA_SETS = {}

# параметри кворуму (на координаторі)
QUORUM = {
    "N": 3,
    "W": 2,
    "R": 1 # за замовчуванням R=1 для наочної демонстрації Round-Robin між 3 репліками; змінюється через POST /cluster/quorum
}

# схеми таблиць: {table_name: {"partition_key": str, "sort_key": str|None}}
TABLES = {}

# локальне сховище даних (зберігає значення разом із версією/offset запису)
# структура: {table_name: {pk: {sk: {"value": any, "version": int, "updated_at": float}}}}
STORAGE = {}

# стан реплікації та логу (для шард-вузлів)
LAST_APPLIED_OFFSET = 0
WAL_READ_EVENTS_LAST_SYNC = 0
RECOVERY_STATS = {
    "recovered_from_disk": False,
    "saved_offset_on_start": 0,
    "skipped_log_entries": 0,
    "caught_up_log_entries": 0,
    "current_offset": 0
}


def get_wal_path(shard_id: str) -> str:
    return os.path.join(DATA_DIR, f"wal_{shard_id}.jsonl")


def get_offset_path(node_id: str) -> str:
    return os.path.join(DATA_DIR, f"offset_{node_id}.json")


def get_state_path(node_id: str) -> str:
    return os.path.join(DATA_DIR, f"state_{node_id}.json")


def http_json_request(method: str, url: str, payload=None, timeout=2.5):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"error": body}
    except Exception as e:
        return 503, {"error": f"Node unreachable: {str(e)}"}


# робота з append-only логом (WAL) та offset

def save_replica_checkpoint():
    """Зберігає поточний offset та стан репліки на диск для швидкого відновлення після рестарту."""
    if ROLE != "shard":
        return
    offset_file = get_offset_path(NODE_ID)
    state_file = get_state_path(NODE_ID)
    with open(offset_file, "w", encoding="utf-8") as f:
        json.dump({
            "node_id": NODE_ID,
            "shard_id": SHARD_ID,
            "replica_role": REPLICA_ROLE,
            "last_applied_offset": LAST_APPLIED_OFFSET,
            "timestamp": time.time()
        }, f, ensure_ascii=False, indent=2)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump({
            "tables": TABLES,
            "storage": STORAGE
        }, f, ensure_ascii=False)


def apply_wal_event_to_memory(event: dict) -> bool:
    """Застосовує одну подію з логу до локальної пам'яті STORAGE, якщо її offset новіший."""
    global LAST_APPLIED_OFFSET
    with STATE_LOCK:
        offset = int(event["offset"])
        if offset <= LAST_APPLIED_OFFSET:
            return False

        op = event.get("op")
        table = event.get("table")
        pk = event.get("pk")
        sk = event.get("sk") or "__default__"
        version = int(event.get("version", offset))
        ts = event.get("timestamp", time.time())

        if op == "REGISTER_TABLE":
            schema = event.get("schema", {})
            TABLES[table] = schema
            STORAGE.setdefault(table, {})
        elif op == "CREATE":
            val = event.get("value")
            STORAGE.setdefault(table, {}).setdefault(pk, {})
            STORAGE[table][pk][sk] = {
                "value": val,
                "version": version,
                "updated_at": ts
            }
        elif op == "DELETE":
            if table in STORAGE and pk in STORAGE[table]:
                if event.get("sk"):
                    STORAGE[table][pk].pop(sk, None)
                    if not STORAGE[table][pk]:
                        del STORAGE[table][pk]
                else:
                    del STORAGE[table][pk]

        LAST_APPLIED_OFFSET = offset
        save_replica_checkpoint()
        return True


def append_to_wal(op: str, table: str, pk: str = None, sk: str = None, value=None, schema=None) -> dict:
    """Лідер додає новий запис у кінець append-only файлу логу (WAL) та застосовує його в себе."""
    global LAST_APPLIED_OFFSET
    with STATE_LOCK:
        new_offset = LAST_APPLIED_OFFSET + 1
        event = {
            "offset": new_offset,
            "version": new_offset,
            "timestamp": round(time.time(), 4),
            "leader_id": NODE_ID,
            "shard_id": SHARD_ID,
            "op": op,
            "table": table,
            "pk": pk,
            "sk": sk,
            "value": value,
            "schema": schema
        }
        wal_path = get_wal_path(SHARD_ID)
        with open(wal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            f.flush()

        apply_wal_event_to_memory(event)
        return event


def read_wal_entries(from_offset: int = 0) -> tuple:
    """Читає події з WAL-файлу, у яких offset > from_offset. Повертає (список_нових, кількість_пропущених)."""
    wal_path = get_wal_path(SHARD_ID)
    if not os.path.exists(wal_path):
        return [], 0
    entries = []
    skipped = 0
    with open(wal_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if int(ev["offset"]) <= from_offset:
                skipped += 1
            else:
                entries.append(ev)
    return entries, skipped


def sync_follower_from_log() -> dict:
    """Фолловер досинхронізовується з WAL (локального файлу або ендпоінту /wal лідера) лише з поточного offset."""
    global WAL_READ_EVENTS_LAST_SYNC
    with STATE_LOCK:
        start_offset = LAST_APPLIED_OFFSET
        entries, skipped = read_wal_entries(from_offset=start_offset)

        # якщо WAL на іншому хості і локального файлу немає - запитуємо у лідера по HTTP
        if not entries and LEADER_URL and not os.path.exists(get_wal_path(SHARD_ID)):
            status, resp = http_json_request("GET", f"{LEADER_URL}/wal?from_offset={start_offset}")
            if status == 200:
                entries = resp.get("entries", [])
                skipped = resp.get("skipped_entries", 0)

        applied_count = 0
        for ev in entries:
            if apply_wal_event_to_memory(ev):
                applied_count += 1

        WAL_READ_EVENTS_LAST_SYNC = applied_count
        return {
            "node_id": NODE_ID,
            "previous_offset": start_offset,
            "skipped_entries": skipped,
            "applied_entries": applied_count,
            "current_offset": LAST_APPLIED_OFFSET
        }


def initialize_node_state_on_startup():
    """Відновлює збережений offset і стан репліки при старті та дочитує лише хвіст логу."""
    global LAST_APPLIED_OFFSET, TABLES, STORAGE, RECOVERY_STATS
    if ROLE != "shard":
        return

    offset_file = get_offset_path(NODE_ID)
    state_file = get_state_path(NODE_ID)
    saved_offset = 0

    if os.path.exists(offset_file) and os.path.exists(state_file):
        try:
            with open(offset_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
                saved_offset = int(meta.get("last_applied_offset", 0))
            with open(state_file, "r", encoding="utf-8") as f:
                st = json.load(f)
                TABLES = st.get("tables", {})
                STORAGE = st.get("storage", {})
            LAST_APPLIED_OFFSET = saved_offset
            RECOVERY_STATS["recovered_from_disk"] = True
        except Exception as e:
            print(f"[{NODE_ID}] Помилка читання збереженого стану: {e}")

    RECOVERY_STATS["saved_offset_on_start"] = saved_offset
    sync_res = sync_follower_from_log()
    RECOVERY_STATS["skipped_log_entries"] = sync_res["skipped_entries"]
    RECOVERY_STATS["caught_up_log_entries"] = sync_res["applied_entries"]
    RECOVERY_STATS["current_offset"] = LAST_APPLIED_OFFSET

    print(
        f"[RECOVERY] Вузол {NODE_ID} ({REPLICA_ROLE}): стартовий збережений offset={saved_offset}, "
        f"пропущено старих записів логу={sync_res['skipped_entries']}, "
        f"досинхронізовано нових записів={sync_res['applied_entries']}, "
        f"актуальний offset={LAST_APPLIED_OFFSET}"
    )


def follower_background_worker():
    """Фоновий потік фолловера, який періодично перевіряє нові записи в лозі."""
    while True:
        try:
            if ROLE == "shard" and REPLICA_ROLE == "follower":
                sync_follower_from_log()
        except Exception:
            pass
        time.sleep(0.5)


# HTTP обробник запитів

class KVRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # тихий лог для чистоти виводу бенчмарків
        pass

    def _send(self, status: int, body: dict):
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # GET запити
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        # статус вузла: GET /health
        if not path_parts or path_parts == ["health"]:
            if ROLE == "coordinator":
                return self._send(200, {
                    "node_id": NODE_ID,
                    "role": ROLE,
                    "mode": "replicated-sharded" if RING.shards else "single-node",
                    "active_shards": len(RING.shards),
                    "quorum": QUORUM,
                    "strict_consistency": (QUORUM["R"] + QUORUM["W"]) > QUORUM["N"],
                    "tables": TABLES
                })
            else:
                return self._send(200, {
                    "node_id": NODE_ID,
                    "role": ROLE,
                    "shard_id": SHARD_ID,
                    "replica_role": REPLICA_ROLE,
                    "last_applied_offset": LAST_APPLIED_OFFSET,
                    "recovery_stats": RECOVERY_STATS,
                    "tables": list(TABLES.keys())
                })

        # список таблиць: GET /tables
        if path_parts == ["tables"]:
            return self._send(200, {"tables": TABLES})

        # топологія шардів та реплік: GET /shards
        if path_parts == ["shards"]:
            return self._send(200, {
                "mode": "replicated-sharded" if RING.shards else "single-node",
                "virtual_nodes_per_shard": RING.virtual_nodes,
                "total_vnodes_on_ring": len(RING.sorted_keys),
                "quorum": QUORUM,
                "strict_quorum_rule": f"R({QUORUM['R']}) + W({QUORUM['W']}) > N({QUORUM['N']}) is {(QUORUM['R'] + QUORUM['W']) > QUORUM['N']}",
                "replica_sets": REPLICA_SETS
            })

        # налаштування кворуму: GET /cluster/quorum
        if path_parts == ["cluster", "quorum"]:
            return self._send(200, {
                "quorum": QUORUM,
                "formula": f"R({QUORUM['R']}) + W({QUORUM['W']}) = {QUORUM['R'] + QUORUM['W']} > N({QUORUM['N']})",
                "strict_consistency_guaranteed": (QUORUM["R"] + QUORUM["W"]) > QUORUM["N"]
            })

        # ендпоінт читання WAL-логу: GET /wal?from_offset=0
        if path_parts == ["wal"]:
            from_offset = int(query.get("from_offset", ["0"])[0])
            entries, skipped = read_wal_entries(from_offset=from_offset)
            return self._send(200, {
                "node_id": NODE_ID,
                "shard_id": SHARD_ID,
                "wal_file": get_wal_path(SHARD_ID),
                "from_offset": from_offset,
                "skipped_entries": skipped,
                "returned_entries_count": len(entries),
                "last_applied_offset": LAST_APPLIED_OFFSET,
                "entries": entries
            })

        # внутрішній дамп стану репліки: GET /internal/dump
        if path_parts == ["internal", "dump"]:
            return self._send(200, {
                "node_id": NODE_ID,
                "shard_id": SHARD_ID,
                "replica_role": REPLICA_ROLE,
                "last_applied_offset": LAST_APPLIED_OFFSET,
                "recovery_stats": RECOVERY_STATS,
                "storage": STORAGE
            })

        # читання запису або перевірка EXISTS: /tables/{table}/items/{pk}[/exists]
        if len(path_parts) >= 4 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = urllib.parse.unquote(path_parts[3])
            is_exists_op = (len(path_parts) == 5 and path_parts[4] == "exists")

            if ROLE == "coordinator" and RING.shards:
                return self._coordinator_handle_read(table, pk, sk, is_exists_op)

            return self._handle_local_read_or_exists(table, pk, sk, is_exists_op)

        return self._send(404, {"error": "Endpoint not found"})

    # POST запити
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        query = urllib.parse.parse_qs(parsed.query)
        body = self._read_body()

        # налаштування параметрів кворуму: POST /cluster/quorum
        if path_parts == ["cluster", "quorum"] and ROLE == "coordinator":
            n = int(body.get("n", QUORUM["N"]))
            w = int(body.get("w", QUORUM["W"]))
            r = int(body.get("r", QUORUM["R"]))
            if w < 1 or r < 1 or w > n or r > n:
                return self._send(400, {"error": "Invalid quorum parameters: must satisfy 1 <= W, R <= N"})
            QUORUM["N"] = n
            QUORUM["W"] = w
            QUORUM["R"] = r
            is_strict = (r + w) > n
            return self._send(200, {
                "message": "Quorum configuration updated",
                "quorum": QUORUM,
                "rule_check": f"R({r}) + W({w}) = {r + w} {'>' if is_strict else '<='} N({n})",
                "strict_quorum_enabled": is_strict
            })

        # реєстрація таблиці: POST /tables
        if path_parts == ["tables"]:
            t_name = body.get("table_name")
            pk_name = body.get("partition_key")
            sk_name = body.get("sort_key")
            if not t_name or not pk_name:
                return self._send(400, {"error": "table_name and partition_key are required"})

            schema = {"partition_key": pk_name, "sort_key": sk_name}
            TABLES[t_name] = schema
            STORAGE.setdefault(t_name, {})

            if ROLE == "coordinator":
                for s_id, rset in REPLICA_SETS.items():
                    if rset.get("leader"):
                        http_json_request("POST", f"{rset['leader']}/tables", body)
            elif ROLE == "shard" and REPLICA_ROLE == "leader":
                append_to_wal("REGISTER_TABLE", table=t_name, schema=schema)
                self._replicate_to_followers()

            return self._send(201, {
                "message": f"Table '{t_name}' registered",
                "schema": schema,
                "node_id": NODE_ID
            })

        # реєстрація репліки шарда в координаторі: POST /shards/register
        if path_parts == ["shards", "register"] and ROLE == "coordinator":
            shard_id = body.get("shard_id")
            node_id = body.get("node_id", shard_id)
            replica_role = body.get("replica_role", "leader")
            url = body.get("url")
            if not shard_id or not url:
                return self._send(400, {"error": "shard_id and url required"})

            with STATE_LOCK:
                RING.add_shard(shard_id)
                rset = REPLICA_SETS.setdefault(shard_id, {
                    "leader": None,
                    "leader_id": None,
                    "replicas": {},
                    "rr_index": 0
                })
                rset["replicas"][node_id] = {
                    "node_id": node_id,
                    "role": replica_role,
                    "url": url
                }
                if replica_role == "leader":
                    rset["leader"] = url
                    rset["leader_id"] = node_id
                    for t_name, schema in TABLES.items():
                        http_json_request("POST", f"{url}/tables", {
                            "table_name": t_name,
                            "partition_key": schema["partition_key"],
                            "sort_key": schema["sort_key"]
                        })

                # повідомляємо лідеру актуальний список URL фолловерів цього шарда
                self._push_followers_to_leader(shard_id)

            return self._send(200, {
                "message": f"Replica '{node_id}' ({replica_role}) registered in '{shard_id}'",
                "shard_id": shard_id,
                "replicas_count": len(REPLICA_SETS[shard_id]["replicas"]),
                "replica_set": REPLICA_SETS[shard_id]
            })

        # внутрішнє оновлення списку фолловерів на лідері: POST /internal/followers
        if path_parts == ["internal", "followers"] and ROLE == "shard":
            self.server.followers = body.get("followers", [])
            return self._send(200, {"status": "followers_updated", "followers": self.server.followers})

        # примусова досинхронізація фолловера з WAL: POST /internal/sync
        if path_parts == ["internal", "sync"] and ROLE == "shard":
            sync_info = sync_follower_from_log()
            return self._send(200, sync_info)

        # CREATE запису: POST /tables/{table}/items
        if len(path_parts) == 3 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = str(body.get("pk", ""))
            sk = body.get("sk")
            val = body.get("value")
            if not pk or val is None:
                return self._send(400, {"error": "Fields 'pk' and 'value' are required"})

            if ROLE == "coordinator" and RING.shards:
                return self._coordinator_handle_write(table, pk, sk, val)

            w_req = int(query.get("w", ["1"])[0])
            return self._shard_leader_handle_create(table, pk, sk, val, w_req)

        return self._send(404, {"error": "Endpoint not found"})

    # DELETE запити
    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        if len(path_parts) == 4 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = urllib.parse.unquote(path_parts[3])

            if ROLE == "coordinator" and RING.shards:
                shard_id, key_hash = RING.get_shard_id(pk)
                rset = REPLICA_SETS.get(shard_id, {})
                leader_url = rset.get("leader")
                if not leader_url:
                    return self._send(503, {"error": f"No leader available for {shard_id}"})
                w_req = QUORUM["W"]
                q_params = f"?w={w_req}" + (f"&sk={urllib.parse.quote(sk)}" if sk else "")
                status, resp = http_json_request(
                    "DELETE",
                    f"{leader_url}/tables/{table}/items/{urllib.parse.quote(pk)}{q_params}"
                )
                resp["routed_to_shard"] = shard_id
                resp["key_hash"] = str(key_hash)
                return self._send(status, resp)

            w_req = int(query.get("w", ["1"])[0])
            return self._shard_leader_handle_delete(table, pk, sk, w_req)

        return self._send(404, {"error": "Endpoint not found"})

    # лоогіка координатора

    def _push_followers_to_leader(self, shard_id: str):
        rset = REPLICA_SETS.get(shard_id, {})
        leader_url = rset.get("leader")
        if not leader_url:
            return
        followers = [
            info for info in rset["replicas"].values()
            if info["role"] == "follower"
        ]
        http_json_request("POST", f"{leader_url}/internal/followers", {"followers": followers})

    def _coordinator_handle_write(self, table: str, pk: str, sk, val):
        if table not in TABLES:
            return self._send(404, {"error": f"Table '{table}' is not registered"})
        schema = TABLES[table]
        if schema.get("sort_key") and not sk:
            return self._send(400, {"error": f"Table '{table}' requires sort_key ('{schema['sort_key']}')"})

        shard_id, key_hash = RING.get_shard_id(pk)
        rset = REPLICA_SETS.get(shard_id, {})
        leader_url = rset.get("leader")
        if not leader_url:
            return self._send(503, {"error": f"Leader for '{shard_id}' is not registered"})

        w_req = QUORUM["W"]
        status, resp = http_json_request(
            "POST",
            f"{leader_url}/tables/{table}/items?w={w_req}",
            {"pk": pk, "sk": sk, "value": val}
        )
        resp["routed_to_shard"] = shard_id
        resp["leader_url"] = leader_url
        resp["key_hash"] = str(key_hash)
        resp["quorum_config"] = QUORUM
        return self._send(status, resp)

    def _coordinator_handle_read(self, table: str, pk: str, sk, is_exists_op: bool):
        if table not in TABLES:
            return self._send(404, {"error": f"Table '{table}' not found"})

        shard_id, key_hash = RING.get_shard_id(pk)
        rset = REPLICA_SETS.get(shard_id, {})
        replicas_list = list(rset.get("replicas", {}).values())
        if not replicas_list:
            return self._send(503, {"error": f"No replicas registered for '{shard_id}'"})

        # визначаємо скільки реплік треба опитати (R) за допомогою Round-Robin по всіх живих репліках
        r_req = 1 if is_exists_op else QUORUM["R"]
        suffix = "/exists" if is_exists_op else ""
        q_str = f"?sk={urllib.parse.quote(sk)}" if sk else ""

        with STATE_LOCK:
            start_idx = rset.get("rr_index", 0)
            rset["rr_index"] = (start_idx + 1) % len(replicas_list)

        ordered_candidates = [
            replicas_list[(start_idx + i) % len(replicas_list)]
            for i in range(len(replicas_list))
        ]

        successful_responses = []
        dead_replicas = []

        for rep in ordered_candidates:
            target_url = f"{rep['url']}/tables/{table}/items/{urllib.parse.quote(pk)}{suffix}{q_str}"
            status, resp = http_json_request("GET", target_url, timeout=1.5)
            if status in (200, 404):
                successful_responses.append((status, resp, rep))
                if len(successful_responses) >= r_req:
                    break
            else:
                dead_replicas.append(rep["node_id"])

        if len(successful_responses) < r_req:
            return self._send(503, {
                "operation": "READ",
                "error": f"Read Quorum not met: needed R={r_req}, got {len(successful_responses)} alive replicas",
                "dead_replicas_skipped": dead_replicas,
                "quorum": QUORUM
            })

        # якщо R=1 (або операція EXISTS) - повертаємо відповідь обраної живої репліки (балансування читань)
        if r_req == 1 or is_exists_op:
            status, best_resp, chosen_rep = successful_responses[0]
            best_resp["routed_by"] = NODE_ID
            best_resp["routed_to_shard"] = shard_id
            best_resp["read_from_node"] = chosen_rep["node_id"]
            best_resp["read_from_role"] = chosen_rep["role"]
            best_resp["replica_url"] = chosen_rep["url"]
            if dead_replicas:
                best_resp["dead_replicas_skipped"] = dead_replicas
            return self._send(status, best_resp)

        # якщо R > 1 (Кворумне читання): обираємо відповідь із максимальною версією (version)
        versions_seen = {}
        best_tuple = None
        max_version = -1

        for status, resp, rep in successful_responses:
            ver = int(resp.get("version", 0)) if status == 200 else 0
            versions_seen[rep["node_id"]] = ver
            if ver > max_version or best_tuple is None:
                max_version = ver
                best_tuple = (status, resp, rep)

        status, best_resp, chosen_rep = best_tuple
        best_resp["routed_by"] = NODE_ID
        best_resp["routed_to_shard"] = shard_id
        best_resp["read_from_node"] = chosen_rep["node_id"]
        best_resp["read_from_role"] = chosen_rep["role"]
        best_resp["quorum_read"] = {
            "N": QUORUM["N"],
            "R": r_req,
            "W": QUORUM["W"],
            "strict_quorum_satisfied": (r_req + QUORUM["W"]) > QUORUM["N"],
            "queried_replicas": [r["node_id"] for _, _, r in successful_responses],
            "versions_seen": versions_seen,
            "selected_latest_version": max_version,
            "selected_from_node": chosen_rep["node_id"],
            "dead_replicas_skipped": dead_replicas
        }
        return self._send(status, best_resp)

    # логіка реплік шарда

    def _replicate_to_followers(self, w_req: int = 3) -> list:
        """Лідер синхронізує фолловерів із WAL-логом, доки не набереться кворум w_req."""
        acked_nodes = [NODE_ID]
        if len(acked_nodes) >= w_req:
            return acked_nodes  # W=1: асинхронний режим, фолловери дочитають у фоні
        followers = getattr(self.server, "followers", [])
        for flw in followers:
            st, sync_resp = http_json_request("POST", f"{flw['url']}/internal/sync", {}, timeout=1.5)
            if st == 200 and int(sync_resp.get("current_offset", 0)) >= LAST_APPLIED_OFFSET:
                acked_nodes.append(flw["node_id"])
                if len(acked_nodes) >= w_req:
                    break  # W=2: достатньо 1 фолловера + лідер
        return acked_nodes

    def _shard_leader_handle_create(self, table: str, pk: str, sk, val, w_req: int):
        if ROLE == "shard" and REPLICA_ROLE != "leader":
            return self._send(403, {"error": f"Node '{NODE_ID}' is a follower. Writes must go to leader."})

        if ROLE == "shard":
            ev = append_to_wal("CREATE", table=table, pk=pk, sk=sk, value=val)
            acked_nodes = self._replicate_to_followers(w_req)
            quorum_met = len(acked_nodes) >= w_req
            http_code = 201 if quorum_met else 503
            return self._send(http_code, {
                "operation": "CREATE",
                "status": "created" if quorum_met else "quorum_failed",
                "shard_id": SHARD_ID,
                "leader_node": NODE_ID,
                "wal_offset": ev["offset"],
                "version": ev["version"],
                "table": table,
                "pk": pk,
                "sk": sk,
                "value": val,
                "replication": {
                    "required_W": w_req,
                    "acks_received": len(acked_nodes),
                    "acked_by_replicas": acked_nodes,
                    "quorum_met": quorum_met
                }
            })

        # fallback для режиму однієї ноди (Single-Node)
        STORAGE.setdefault(table, {}).setdefault(pk, {})
        sk_key = sk if sk else "__default__"
        STORAGE[table][pk][sk_key] = {"value": val, "version": 1, "updated_at": time.time()}
        return self._send(201, {
            "operation": "CREATE",
            "status": "created",
            "stored_on_node": NODE_ID,
            "mode": "single-node",
            "table": table,
            "pk": pk,
            "sk": sk,
            "value": val
        })

    def _shard_leader_handle_delete(self, table: str, pk: str, sk, w_req: int):
        if ROLE == "shard" and REPLICA_ROLE != "leader":
            return self._send(403, {"error": "Writes/Deletes must go to leader"})

        table_data = STORAGE.get(table, {})
        if pk not in table_data:
            return self._send(404, {"operation": "DELETE", "error": "Key not found", "pk": pk, "node": NODE_ID})

        ev = append_to_wal("DELETE", table=table, pk=pk, sk=sk)
        acked_nodes = self._replicate_to_followers(w_req)
        quorum_met = len(acked_nodes) >= w_req
        return self._send(200 if quorum_met else 503, {
            "operation": "DELETE",
            "deleted": quorum_met,
            "pk": pk,
            "sk": sk,
            "wal_offset": ev["offset"],
            "version": ev["version"],
            "acked_by_replicas": acked_nodes
        })

    def _handle_local_read_or_exists(self, table: str, pk: str, sk, is_exists_op: bool):
        table_data = STORAGE.get(table, {})
        pk_data = table_data.get(pk)

        if not pk_data:
            if is_exists_op:
                return self._send(200, {
                    "operation": "EXISTS", "exists": False, "pk": pk, "sk": sk,
                    "checked_on_node": NODE_ID, "replica_role": REPLICA_ROLE
                })
            return self._send(404, {
                "operation": "READ", "error": "Key not found", "pk": pk, "sk": sk,
                "checked_on_node": NODE_ID, "replica_role": REPLICA_ROLE
            })

        if sk:
            exists = sk in pk_data
            if is_exists_op:
                return self._send(200, {
                    "operation": "EXISTS", "exists": exists, "pk": pk, "sk": sk,
                    "checked_on_node": NODE_ID, "replica_role": REPLICA_ROLE
                })
            if not exists:
                return self._send(404, {
                    "operation": "READ", "error": "Sort key not found", "pk": pk, "sk": sk,
                    "checked_on_node": NODE_ID
                })
            entry = pk_data[sk]
            return self._send(200, {
                "operation": "READ",
                "pk": pk,
                "sk": sk,
                "value": entry["value"],
                "version": entry["version"],
                "stored_on_node": NODE_ID,
                "replica_role": REPLICA_ROLE,
                "replica_offset": LAST_APPLIED_OFFSET
            })
        else:
            if is_exists_op:
                return self._send(200, {
                    "operation": "EXISTS", "exists": True, "pk": pk,
                    "checked_on_node": NODE_ID, "replica_role": REPLICA_ROLE
                })
            if "__default__" in pk_data and len(pk_data) == 1:
                entry = pk_data["__default__"]
                return self._send(200, {
                    "operation": "READ",
                    "pk": pk,
                    "value": entry["value"],
                    "version": entry["version"],
                    "stored_on_node": NODE_ID,
                    "replica_role": REPLICA_ROLE,
                    "replica_offset": LAST_APPLIED_OFFSET
                })

            sorted_items = {k: pk_data[k]["value"] for k in sorted(pk_data.keys())}
            max_ver = max((pk_data[k]["version"] for k in pk_data), default=0)
            return self._send(200, {
                "operation": "READ_PARTITION",
                "pk": pk,
                "items_count": len(sorted_items),
                "items_sorted_by_sk": sorted_items,
                "version": max_ver,
                "stored_on_node": NODE_ID,
                "replica_role": REPLICA_ROLE,
                "replica_offset": LAST_APPLIED_OFFSET
            })


def register_with_coordinator(coordinator_url: str, shard_id: str, node_id: str, replica_role: str, my_url: str):
    for attempt in range(1, 6):
        status, resp = http_json_request(
            "POST",
            f"{coordinator_url}/shards/register",
            {
                "shard_id": shard_id,
                "node_id": node_id,
                "replica_role": replica_role,
                "url": my_url
            }
        )
        if status == 200:
            print(f"[{node_id}] Успішно зареєстровано в координаторі ({replica_role} для {shard_id})")
            return
        print(f"[{node_id}] Спроба {attempt}/5 реєстрації не вдалася ({status}), чекаємо 2с...")
        time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Replicated KV Store Node (Milestone 3)")
    parser.add_argument("--role", choices=["coordinator", "shard"], required=True)
    parser.add_argument("--replica-role", choices=["leader", "follower"], default="leader")
    parser.add_argument("--shard-id", default="shard-1", help="Логічний ID шарда (наприклад, shard-1)")
    parser.add_argument("--node-id", default="node-1", help="Унікальний ID вузла (наприклад, shard-1-leader)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--coordinator-url", help="URL координатора")
    parser.add_argument("--advertise-url", help="Власний URL вузла")
    parser.add_argument("--leader-url", help="URL лідера шарда (для фолловерів)")
    parser.add_argument("--data-dir", default=os.path.expanduser("~"), help="Директорія для WAL та offset файлів")
    args = parser.parse_args()

    ROLE = args.role
    REPLICA_ROLE = args.replica_role
    SHARD_ID = args.shard_id
    NODE_ID = args.node_id
    DATA_DIR = os.path.expanduser(args.data_dir)
    LEADER_URL = args.leader_url

    if ROLE == "shard":
        initialize_node_state_on_startup()
        if REPLICA_ROLE == "follower":
            t = threading.Thread(target=follower_background_worker, daemon=True)
            t.start()

    server = ThreadingHTTPServer((args.host, args.port), KVRequestHandler)
    server.followers = []
    print(f"[{ROLE.upper()}] Вузол '{NODE_ID}' (shard={SHARD_ID}, replica_role={REPLICA_ROLE}) запущено на {args.host}:{args.port}")

    if ROLE == "shard" and args.coordinator_url and args.advertise_url:
        threading.Thread(
            target=lambda: (time.sleep(0.5), register_with_coordinator(args.coordinator_url, SHARD_ID, NODE_ID, REPLICA_ROLE, args.advertise_url)),
            daemon=True
        ).start()

    server.serve_forever()