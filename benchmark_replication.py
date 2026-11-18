#!/usr/bin/env python3
import json
import statistics
import time
import urllib.request

COORDINATOR_URL = "http://localhost:8000"
TABLE_NAME = "bench_table"
NUM_OPERATIONS = 200 # 200 записів + 200 читань у кожному режимі


def http_call(method: str, path: str, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        f"{COORDINATOR_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def percentile(sorted_list, p: float) -> float:
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_list) - 1)
    if f == c:
        return sorted_list[int(k)]
    return sorted_list[f] * (c - k) + sorted_list[c] * (k - f)


def run_phase(mode_title: str, n: int, w: int, r: int):
    # встановлюємо режим кворуму
    http_call("POST", "/cluster/quorum", {"n": n, "w": w, "r": r})

    write_latencies = []
    t_start_write = time.perf_counter()
    for i in range(NUM_OPERATIONS):
        t0 = time.perf_counter()
        http_call("POST", f"/tables/{TABLE_NAME}/items", {
            "pk": f"bench_user_{i}",
            "value": {"seq": i, "payload": "data_" + str(i)}
        })
        write_latencies.append((time.perf_counter() - t0) * 1000.0)
    total_write_time = time.perf_counter() - t_start_write

    read_latencies = []
    t_start_read = time.perf_counter()
    for i in range(NUM_OPERATIONS):
        t0 = time.perf_counter()
        http_call("GET", f"/tables/{TABLE_NAME}/items/bench_user_{i}")
        read_latencies.append((time.perf_counter() - t0) * 1000.0)
    total_read_time = time.perf_counter() - t_start_read

    w_sorted = sorted(write_latencies)
    r_sorted = sorted(read_latencies)

    return {
        "title": mode_title,
        "quorum": f"N={n}, W={w}, R={r}",
        "w_throughput": NUM_OPERATIONS / total_write_time,
        "w_avg": statistics.mean(w_sorted),
        "w_p50": percentile(w_sorted, 50),
        "w_p95": percentile(w_sorted, 95),
        "w_p99": percentile(w_sorted, 99),
        "r_throughput": NUM_OPERATIONS / total_read_time,
        "r_avg": statistics.mean(r_sorted),
        "r_p50": percentile(r_sorted, 50),
        "r_p95": percentile(r_sorted, 95),
        "r_p99": percentile(r_sorted, 99),
    }


def main():
    print("=" * 86)
    print(f" НАВАНТАЖУВАЛЬНИЙ ТЕСТ (YCSB-like Workload: {NUM_OPERATIONS} WRITE + {NUM_OPERATIONS} READ)")
    print("=" * 86)

    http_call("POST", "/tables", {"table_name": TABLE_NAME, "partition_key": "user_id"})

    modes = [
        ("1. Без очікування реплік (W=1, R=1) [Baseline / Асинхронний]", 3, 1, 1),
        ("2. Кворумна реплікація (W=2, R=2, R+W>N) [Strong Consistency]", 3, 2, 2),
        ("3. Повна синхронізація всіх реплік (W=3, R=1) [All-Ack Write]", 3, 3, 1),
    ]

    results = []
    for title, n, w, r in modes:
        res = run_phase(title, n, w, r)
        results.append(res)
        print(f"\n>>> {res['title']} ({res['quorum']})")
        print(f"  [WRITE] Throughput: {res['w_throughput']:6.1f} ops/sec | "
              f"Avg: {res['w_avg']:5.2f} ms | p50: {res['w_p50']:5.2f} ms | p95: {res['w_p95']:5.2f} ms | p99: {res['w_p99']:5.2f} ms")
        print(f"  [READ]  Throughput: {res['r_throughput']:6.1f} ops/sec | "
              f"Avg: {res['r_avg']:5.2f} ms | p50: {res['r_p50']:5.2f} ms | p95: {res['r_p95']:5.2f} ms | p99: {res['r_p99']:5.2f} ms")

    # повертаємо стандартний режим кворуму N=3, W=2, R=2
    http_call("POST", "/cluster/quorum", {"n": 3, "w": 2, "r": 2})
    print("\n" + "=" * 86)


if __name__ == "__main__":
    main()