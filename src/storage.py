def volume_path(catalog, schema, volume, relative_path=""):
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    rel = relative_path.strip("/")
    return f"{base}/{rel}" if rel else base

def abfss_uri(container, storage_account, relative_path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    rel = relative_path.strip("/")
    return f"{base}/{rel}" if rel else base
