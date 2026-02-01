import psycopg2  # type: ignore


# 这里基于原版 dbconnnection.Database 的思路，
# 选取一小组我们认为比较重要、且在 AlloyDB/PostgreSQL 中常见的 knob：
# - max_connections
# - shared_buffers
# - work_mem
# - maintenance_work_mem
# - effective_cache_size
# - google_columnar_engine.refresh_threshold_percentage（与本实验高度相关）
#
# 并为每个 knob 预设一个 [min_value, max_value] 区间，用于做简单归一化：
#   norm = (value - min) / (max - min)

KNOB_CONFIG = {
    "max_connections": {"min": 10, "max": 1000},
    "shared_buffers": {"min": 4 * 1024 * 1024, "max": 16 * 1024 * 1024 * 1024},  # 4MB ~ 16GB
    "work_mem": {"min": 1 * 1024 * 1024, "max": 1 * 1024 * 1024 * 1024},         # 1MB ~ 1GB
    "maintenance_work_mem": {"min": 16 * 1024 * 1024, "max": 4 * 1024 * 1024 * 1024},  # 16MB ~ 4GB
    "effective_cache_size": {"min": 128 * 1024 * 1024, "max": 64 * 1024 * 1024 * 1024},  # 128MB ~ 64GB
    "google_columnar_engine.refresh_threshold_percentage": {"min": 0, "max": 100},
}


def _parse_pg_size(value: str) -> float:

    v = value.strip().lower()
    try:
        return float(v)
    except ValueError:
        pass

    units = {
        "kb": 1024.0,
        "k": 1024.0,
        "mb": 1024.0 * 1024.0,
        "m": 1024.0 * 1024.0,
        "gb": 1024.0 * 1024.0 * 1024.0,
        "g": 1024.0 * 1024.0 * 1024.0,
    }

    for u, factor in units.items():
        if v.endswith(u):
            num = float(v[: -len(u)])
            return num * factor

    return float(v)


def fetch_raw_knobs():
    conn = psycopg2.connect(
        host="localhost",
        port=5555,
        user="postgres",
        password="postgres",
        dbname="htap_sf1",
    )
    cur = conn.cursor()

    raw_values = {}
    for name in KNOB_CONFIG.keys():
        cur.execute(f"SHOW {name}")
        val = cur.fetchone()[0]
        if name.endswith("refresh_threshold_percentage"):
            raw_values[name] = float(val)
        else:
            raw_values[name] = _parse_pg_size(val)

    cur.close()
    conn.close()
    return raw_values


def normalize_knobs(raw_values):

    knob_names = []
    norm_values = []

    for name, cfg in KNOB_CONFIG.items():
        v = raw_values.get(name)
        vmin = cfg["min"]
        vmax = cfg["max"]
        if v is None:
            norm = 0.5
        else:
            if vmax <= vmin:
                norm = 0.5
            else:
                norm = (v - vmin) / float(vmax - vmin)
                if norm < 0.0:
                    norm = 0.0
                if norm > 1.0:
                    norm = 1.0

        knob_names.append(name)
        norm_values.append(norm)

    return knob_names, norm_values


if __name__ == "__main__":
    raw = fetch_raw_knobs()
    names, norm = normalize_knobs(raw)

    print("Raw knob values:")
    for n in names:
        print(f"  {n} = {raw[n]}")

    print("\nNormalized knob vector (0~1):")
    print("NAMES =", names)
    print("KNOBS =", norm)


