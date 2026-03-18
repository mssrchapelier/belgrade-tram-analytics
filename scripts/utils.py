from pathlib import Path

def worker_filerenamer(paths: tuple[Path, Path]) -> None:
    paths[0].rename(paths[1])

