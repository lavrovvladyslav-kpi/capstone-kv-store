#!/usr/bin/env python3
import argparse
import bisect
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


class ConsistentHashRing:
    """Реалізація Consistent Hashing з віртуальними вузлами."""
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}          # hash_val -> shard_id
        self.sorted_keys = []   # відсортовані хеші віртуальних вузлів
        self.shards = {}        # shard_id -> url

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_shard(self, shard_id: str, url: str):
        if shard_id in self.shards:
            self.shards[shard_id] = url
            return
        self.shards[shard_id] = url
        for i in range(self.virtual_nodes):
            vnode_key = f"{shard_id}#vnode{i}"
            h = self._hash(vnode_key)
            self.ring[h] = shard_id
            bisect.insort(self.sorted_keys, h)

    def get_shard(self, partition_key: str):
        if not self.sorted_keys:
            return None, None, None
        key_hash = self._hash(partition_key)
        idx = bisect.bisect_right(self.sorted_keys, key_hash)
        if idx == len(self.sorted_keys):
            idx = 0
        vnode_hash = self.sorted_keys[idx]
        shard_id = self.ring[vnode_hash]
        return shard_id, self.shards[shard_id], key_hash


# Стан вузла
ROLE = "coordinator"
NODE_ID = "coordinator-1"
RING = ConsistentHashRing(virtual_nodes=100)

# Схеми таблиць: {table_name: {"partition_key": str, "sort_key": str|None}}
TABLES = {}

# Локальне сховище (використовується шардами АБО координатором у режимі однієї ноди)
# Структура: {table_name: {pk: {sk: value}}} (якщо sort_key немає, sk = "__default__")
STORAGE = {}


def http_json_request(method: str, url: str, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"error": body}


class KVRequestHandler(BaseHTTPRequestHandler):
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

    # -------------------- РОУТИНГ ЗАПИТІВ --------------------
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        # 1. Статус вузла
        if not path_parts or path_parts == ["health"]:
            mode = "single-node" if (ROLE == "coordinator" and not RING.shards) else ROLE
            return self._send(200, {
                "node_id": NODE_ID,
                "role": ROLE,
                "mode": mode,
                "active_shards": len(RING.shards),
                "shards": RING.shards,
                "tables": TABLES
            })

        # 2. Список таблиць
        if path_parts == ["tables"]:
            return self._send(200, {"tables": TABLES})

        # 3. Список шардів
        if path_parts == ["shards"]:
            return self._send(200, {
                "mode": "sharded" if RING.shards else "single-node",
                "virtual_nodes_per_shard": RING.virtual_nodes,
                "total_vnodes_on_ring": len(RING.sorted_keys),
                "shards": RING.shards
            })

        # 4. Внутрішній дамп шарда (для перебалансування при додаванні нового шарда)
        if path_parts == ["internal", "dump"]:
            return self._send(200, {"node_id": NODE_ID, "storage": STORAGE})

        # 5. Операції з записами: /tables/{table}/items/{pk} або /tables/{table}/items/{pk}/exists
        if len(path_parts) >= 4 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = urllib.parse.unquote(path_parts[3])
            is_exists_op = (len(path_parts) == 5 and path_parts[4] == "exists")

            # Якщо ми координатор і є зареєстровані шарди -> маршрутизуємо на шард
            if ROLE == "coordinator" and RING.shards:
                if table not in TABLES:
                    return self._send(404, {"error": f"Table '{table}' not found"})
                shard_id, shard_url, key_hash = RING.get_shard(pk)
                suffix = "/exists" if is_exists_op else ""
                q_str = f"?sk={urllib.parse.quote(sk)}" if sk else ""
                target_url = f"{shard_url}/tables/{table}/items/{urllib.parse.quote(pk)}{suffix}{q_str}"
                status, resp = http_json_request("GET", target_url)
                resp["routed_by"] = NODE_ID
                resp["routed_to_shard"] = shard_id
                resp["shard_url"] = shard_url
                resp["key_hash"] = str(key_hash)
                return self._send(status, resp)

            # Локальне виконання (режим однієї ноди або ми і є цільовий шард)
            return self._handle_local_read_or_exists(table, pk, sk, is_exists_op)

        return self._send(404, {"error": "Endpoint not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        body = self._read_body()

        # 1. Реєстрація таблиці: POST /tables
        if path_parts == ["tables"]:
            t_name = body.get("table_name")
            pk_name = body.get("partition_key")
            sk_name = body.get("sort_key")
            if not t_name or not pk_name:
                return self._send(400, {"error": "table_name and partition_key are required"})
            TABLES[t_name] = {"partition_key": pk_name, "sort_key": sk_name}
            STORAGE.setdefault(t_name, {})
            # Якщо є шарди — синхронізуємо схему таблиці на всі шарди
            if ROLE == "coordinator":
                for s_url in RING.shards.values():
                    http_json_request("POST", f"{s_url}/tables", body)
            return self._send(201, {
                "message": f"Table '{t_name}' registered",
                "schema": TABLES[t_name],
                "node_id": NODE_ID
            })

        # 2. Реєстрація шарда: POST /shards/register
        if path_parts == ["shards", "register"] and ROLE == "coordinator":
            shard_id = body.get("shard_id")
            shard_url = body.get("url")
            if not shard_id or not shard_url:
                return self._send(400, {"error": "shard_id and url required"})

            # Синхронізуємо таблиці на новий шард
            for t_name, schema in TABLES.items():
                http_json_request("POST", f"{shard_url}/tables", {
                    "table_name": t_name,
                    "partition_key": schema["partition_key"],
                    "sort_key": schema["sort_key"]
                })

            old_shards = dict(RING.shards)
            RING.add_shard(shard_id, shard_url)
            moved_keys = self._rebalance_cluster(old_shards, shard_id)

            print(f"[COORDINATOR] Зареєстровано шард {shard_id} ({shard_url}). Перенесено ключів: {moved_keys}")
            return self._send(200, {
                "message": f"Shard '{shard_id}' registered successfully",
                "active_shards": len(RING.shards),
                "shards": RING.shards,
                "rebalanced_keys_count": moved_keys
            })

        # 3. CREATE запису: POST /tables/{table}/items
        if len(path_parts) == 3 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = str(body.get("pk", ""))
            sk = body.get("sk")
            val = body.get("value")
            if not pk or val is None:
                return self._send(400, {"error": "Fields 'pk' and 'value' are required"})

            if ROLE == "coordinator" and RING.shards:
                if table not in TABLES:
                    return self._send(404, {"error": f"Table '{table}' is not registered"})
                schema = TABLES[table]
                if schema.get("sort_key") and not sk:
                    return self._send(400, {"error": f"Table '{table}' requires sort_key ('{schema['sort_key']}')"})

                shard_id, shard_url, key_hash = RING.get_shard(pk)
                status, resp = http_json_request("POST", f"{shard_url}/tables/{table}/items", body)
                resp["mode"] = "sharded"
                resp["routed_to_shard"] = shard_id
                resp["shard_url"] = shard_url
                resp["key_hash"] = str(key_hash)
                return self._send(status, resp)

            # Локальний запис (Single-Node Mode або ми є цільовим шардом)
            return self._handle_local_create(table, pk, sk, val)

        return self._send(404, {"error": "Endpoint not found"})

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        if len(path_parts) == 4 and path_parts[0] == "tables" and path_parts[2] == "items":
            table = path_parts[1]
            pk = urllib.parse.unquote(path_parts[3])

            if ROLE == "coordinator" and RING.shards:
                shard_id, shard_url, key_hash = RING.get_shard(pk)
                q_str = f"?sk={urllib.parse.quote(sk)}" if sk else ""
                status, resp = http_json_request("DELETE", f"{shard_url}/tables/{table}/items/{urllib.parse.quote(pk)}{q_str}")
                resp["routed_to_shard"] = shard_id
                resp["key_hash"] = str(key_hash)
                return self._send(status, resp)

            return self._handle_local_delete(table, pk, sk)

        return self._send(404, {"error": "Endpoint not found"})

    # -------------------- ЛОКАЛЬНІ ОПЕРАЦІЇ СХОВИЩА --------------------
    def _handle_local_create(self, table: str, pk: str, sk, val):
        if ROLE == "coordinator" and table not in TABLES:
            return self._send(404, {"error": f"Table '{table}' is not registered"})
        schema = TABLES.get(table, {"partition_key": "pk", "sort_key": None})
        if schema.get("sort_key") and not sk:
            return self._send(400, {"error": f"Table '{table}' requires sort_key"})

        STORAGE.setdefault(table, {}).setdefault(pk, {})
        sk_key = sk if sk else "__default__"
        STORAGE[table][pk][sk_key] = val
        return self._send(201, {
            "operation": "CREATE",
            "status": "created",
            "stored_on_node": NODE_ID,
            "mode": "single-node" if ROLE == "coordinator" else "shard-storage",
            "table": table,
            "pk": pk,
            "sk": sk,
            "value": val
        })

    def _handle_local_read_or_exists(self, table: str, pk: str, sk, is_exists_op: bool):
        table_data = STORAGE.get(table, {})
        pk_data = table_data.get(pk)

        if not pk_data:
            if is_exists_op:
                return self._send(200, {"operation": "EXISTS", "exists": False, "pk": pk, "sk": sk, "checked_on_node": NODE_ID})
            return self._send(404, {"operation": "READ", "error": "Key not found", "pk": pk, "sk": sk, "checked_on_node": NODE_ID})

        if sk:
            exists = sk in pk_data
            if is_exists_op:
                return self._send(200, {"operation": "EXISTS", "exists": exists, "pk": pk, "sk": sk, "checked_on_node": NODE_ID})
            if not exists:
                return self._send(404, {"operation": "READ", "error": "Sort key not found", "pk": pk, "sk": sk, "checked_on_node": NODE_ID})
            return self._send(200, {
                "operation": "READ",
                "pk": pk,
                "sk": sk,
                "value": pk_data[sk],
                "stored_on_node": NODE_ID
            })
        else:
            if is_exists_op:
                return self._send(200, {"operation": "EXISTS", "exists": True, "pk": pk, "checked_on_node": NODE_ID})
            # Якщо таблиця без sort_key
            if "__default__" in pk_data and len(pk_data) == 1:
                return self._send(200, {
                    "operation": "READ",
                    "pk": pk,
                    "value": pk_data["__default__"],
                    "stored_on_node": NODE_ID
                })
            # Якщо таблиця зі складеним ключем і sk не вказано — повертаємо відсортовані за sk записи партиції
            sorted_items = {k: pk_data[k] for k in sorted(pk_data.keys())}
            return self._send(200, {
                "operation": "READ_PARTITION",
                "pk": pk,
                "items_count": len(sorted_items),
                "items_sorted_by_sk": sorted_items,
                "stored_on_node": NODE_ID
            })

    def _handle_local_delete(self, table: str, pk: str, sk):
        table_data = STORAGE.get(table, {})
        if pk not in table_data:
            return self._send(404, {"operation": "DELETE", "error": "Key not found", "pk": pk, "node": NODE_ID})

        if sk:
            if sk not in table_data[pk]:
                return self._send(404, {"operation": "DELETE", "error": "Sort key not found", "pk": pk, "sk": sk, "node": NODE_ID})
            deleted_val = table_data[pk].pop(sk)
            if not table_data[pk]:
                del table_data[pk]
            return self._send(200, {"operation": "DELETE", "deleted": True, "pk": pk, "sk": sk, "deleted_value": deleted_val, "node": NODE_ID})
        else:
            deleted_val = table_data.pop(pk)
            return self._send(200, {"operation": "DELETE", "deleted": True, "pk": pk, "deleted_value": deleted_val, "node": NODE_ID})

    def _rebalance_cluster(self, old_shards: dict, new_shard_id: str) -> int:
        """Переносить ключі при переході з Single-Node або при додаванні нового шарда."""
        moved = 0
        # 1. Якщо в координаторі були локальні дані (Single-Node Mode) — переносимо на шард
        if not old_shards and STORAGE:
            for t_name, pk_map in list(STORAGE.items()):
                for pk, sk_map in list(pk_map.items()):
                    _, s_url, _ = RING.get_shard(pk)
                    for sk_key, val in sk_map.items():
                        payload = {"pk": pk, "value": val}
                        if sk_key != "__default__":
                            payload["sk"] = sk_key
                        http_json_request("POST", f"{s_url}/tables/{t_name}/items", payload)
                        moved += 1
            STORAGE.clear()
            return moved

        # 2. Якщо додається новий шард до вже існуючих — перевіряємо, які ключі мають переїхати
        for s_id, s_url in old_shards.items():
            status, dump = http_json_request("GET", f"{s_url}/internal/dump")
            if status != 200:
                continue
            shard_storage = dump.get("storage", {})
            for t_name, pk_map in shard_storage.items():
                for pk, sk_map in pk_map.items():
                    target_id, target_url, _ = RING.get_shard(pk)
                    if target_id != s_id:
                        for sk_key, val in sk_map.items():
                            payload = {"pk": pk, "value": val}
                            if sk_key != "__default__":
                                payload["sk"] = sk_key
                            http_json_request("POST", f"{target_url}/tables/{t_name}/items", payload)
                            moved += 1
                        http_json_request("DELETE", f"{s_url}/tables/{t_name}/items/{urllib.parse.quote(pk)}")
        return moved


def register_with_coordinator(coordinator_url: str, shard_id: str, my_url: str):
    for attempt in range(1, 6):
        status, resp = http_json_request(
            "POST",
            f"{coordinator_url}/shards/register",
            {"shard_id": shard_id, "url": my_url}
        )
        if status == 200:
            print(f"[SHARD {shard_id}] Успішно зареєстровано в координаторі {coordinator_url}: {resp}")
            return
        print(f"[SHARD {shard_id}] Спроба {attempt}/5 реєстрації не вдалася ({status}), чекаємо 2с...")
        time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed KV Store Node")
    parser.add_argument("--role", choices=["coordinator", "shard"], required=True)
    parser.add_argument("--node-id", default="node-1")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--coordinator-url", help="URL координатора (для шарда)")
    parser.add_argument("--advertise-url", help="Власний URL шарда, який бачить координатор")
    args = parser.parse_args()

    ROLE = args.role
    NODE_ID = args.node_id

    server = HTTPServer((args.host, args.port), KVRequestHandler)
    print(f"[{ROLE.upper()}] Вузол '{NODE_ID}' запущено на {args.host}:{args.port}")

    if ROLE == "shard" and args.coordinator_url and args.advertise_url:
        register_with_coordinator(args.coordinator_url, NODE_ID, args.advertise_url)

    server.serve_forever()