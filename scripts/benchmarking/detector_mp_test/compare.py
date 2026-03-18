from pathlib import Path

from scripts.stats.paired_t_test import paired_t_test

def compare_profiling_results_for_yaml_dump() -> None:
    # Testing whether dumping `v1.src.models.pipeline_output.PipelineArtefacts`
    # to YAML adds significant overhead, and whether dumping from the Pydantic model
    # is slower than from JSON with the same schema.
    parent_dir: Path = Path(__file__).resolve().parent
    data_path: Path = parent_dir / "data.csv"
    out_path: Path = parent_dir / "results.txt"

    paired_t_test(data_path, out_path)

if __name__ == "__main__":
    compare_profiling_results_for_yaml_dump()