from dataclasses import dataclass
from pathlib import Path
import os
import shutil

SCHEMA_NAME = "retail_training"
VOLUME_NAME = "training_files"

@dataclass(frozen=True)
class LabContext:
    project_root: str
    catalog: str
    schema: str
    volume: str
    volume_root: str
    source_root: str
    output_root: str

    def table(self, table_name: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table_name}`"

def locate_project_root(marker_name="RETAIL_DATABRICKS_27_29_LAB.json"):
    cwd = Path(os.getcwd()).resolve()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / marker_name).exists():
            return str(candidate)

    for search_root in [Path("/Workspace/Users"), Path("/Workspace/Shared"), Path("/Workspace")]:
        if not search_root.exists():
            continue
        for current_root, dirs, files in os.walk(search_root):
            dirs[:] = [d for d in dirs if not d.startswith(".") and d not in {"Trash", ".Trash"}]
            if marker_name in files:
                return current_root

    raise FileNotFoundError(
        "Project marker not found. Import the complete ZIP into Databricks Workspace."
    )

def _q(name):
    return f"`{name.replace('`', '``')}`"

def _copy_sources(project_root, target_root):
    src_root = Path(project_root) / "data" / "official_sources"
    dst_root = Path(target_root)
    ready = dst_root / "_OFFICIAL_SOURCE_READY"
    if ready.exists():
        return

    dst_root.mkdir(parents=True, exist_ok=True)
    for src in src_root.rglob("*"):
        if src.is_file():
            rel = src.relative_to(src_root)
            dst = dst_root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    ready.write_text("ready\n", encoding="utf-8")

def ensure_lab(spark, project_root):
    catalog = spark.sql("SELECT current_catalog() AS c").first()["c"]
    if not catalog:
        raise RuntimeError("No current Unity Catalog catalog is available.")

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(SCHEMA_NAME)}"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {_q(catalog)}.{_q(SCHEMA_NAME)}.{_q(VOLUME_NAME)}"
    )

    volume_root = f"/Volumes/{catalog}/{SCHEMA_NAME}/{VOLUME_NAME}"
    source_root = f"{volume_root}/official_sources"
    output_root = f"{volume_root}/outputs"

    _copy_sources(project_root, source_root)
    Path(output_root).mkdir(parents=True, exist_ok=True)

    return LabContext(
        project_root=project_root,
        catalog=catalog,
        schema=SCHEMA_NAME,
        volume=VOLUME_NAME,
        volume_root=volume_root,
        source_root=source_root,
        output_root=output_root,
    )
