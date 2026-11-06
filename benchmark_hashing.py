#!/usr/bin/env python3
import hashlib
from kv_node import ConsistentHashRing


def modulo_shard(key: str, num_shards: int) -> str:
    h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
    return f"shard-{(h % num_shards) + 1}"


def run_experiment(total_keys=10000, virtual_nodes=100):
    keys = [f"user_key_{i}" for i in range(1, total_keys + 1)]

    # 1. Звичайне хешування (hash % N): перехід з 2 до 3 шардів
    mod_moved = 0
    for k in keys:
        before = modulo_shard(k, 2)
        after = modulo_shard(k, 3)
        if before != after:
            mod_moved += 1

    # 2. Consistent Hashing (з віртуальними вузлами): перехід з 2 до 3 шардів
    ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
    ring.add_shard("shard-1", "http://10.0.11.10:8001")
    ring.add_shard("shard-2", "http://10.0.12.10:8002")

    ch_before = {k: ring.get_shard(k)[0] for k in keys}
    dist_2 = {s: list(ch_before.values()).count(s) for s in ["shard-1", "shard-2"]}

    # Додаємо 3-й шард
    ring.add_shard("shard-3", "http://10.0.11.20:8003")
    ch_after = {k: ring.get_shard(k)[0] for k in keys}
    dist_3 = {s: list(ch_after.values()).count(s) for s in ["shard-1", "shard-2", "shard-3"]}

    ch_moved = sum(1 for k in keys if ch_before[k] != ch_after[k])

    print("=" * 68)
    print(f" ЕКСПЕРИМЕНТ: Додавання 3-го шарда (Кількість ключів = {total_keys})")
    print(f" Кількість віртуальних вузлів на шард (vnodes) = {virtual_nodes}")
    print("=" * 68)
    print(f"1. Звичайний алгоритм (hash % N):")
    print(f"   - Переїхало ключів (2 -> 3 шарди): {mod_moved} із {total_keys} ({mod_moved / total_keys * 100:.2f}%)")
    print(f"   - Теоретичне очікування для hash % N: ~66.67% (2/3 ключів)")
    print("-" * 68)
    print(f"2. Consistent Hashing (віртуальні вузли = {virtual_nodes}):")
    print(f"   - Розподіл на 2 шардах: {dist_2}")
    print(f"   - Розподіл на 3 шардах: {dist_3}")
    print(f"   - Переїхало ключів (2 -> 3 шарди): {ch_moved} із {total_keys} ({ch_moved / total_keys * 100:.2f}%)")
    print(f"   - Теоретичний ідеал (1/N):         ~33.33% (1/3 ключів)")
    print("=" * 68)


if __name__ == "__main__":
    run_experiment(total_keys=10000, virtual_nodes=100)