import subprocess
import os

def create_uml(*, name: str, src_path: str, out_dir: str, dest_format: str):
    os.makedirs(out_dir, exist_ok=True)
    cmd: str = f"pyreverse -o {dest_format} -p {name} {src_path}"
    subprocess.run(cmd, shell=True, cwd=out_dir)
    print(f"Saved to: {out_dir}")
