def configure_cpu_inference_runtime():
    """
    Configure environment values for CPU-bound numerical inference.

    Rationale:
    To prevent thread oversubscription.
    Not setting these envvars caused YOLO object detectors,
    when running in separate processes in parallel,
    to spend multiple seconds on a single prediction call.
    """
    import os

    # Can try to experiment and benchmark with SLIGHTLY larger values

    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    os.environ.setdefault("OMP_WAIT_POLICY", "PASSIVE")
