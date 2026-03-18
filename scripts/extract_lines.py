from _io import TextIOWrapper

def extract_lines(src_path: str, dest_path: str,
                  *, first_line_idx: int, last_line_idx: int) -> None:
    """
    Extract the specified lines from the source file and dump into destination.
    Useful to later browse just a portion of files that are too large to comfortably load
    into a text editor.
    """
    with (
            open(src_path, "r", encoding="utf8") as fin,
            open(dest_path, "w", encoding="utf8") as fout
    ): # type: TextIOWrapper, TextIOWrapper
        first_idx_written: int | None = None
        last_idx_written: int | None = None
        for idx, src_line in enumerate(fin): # type: int, str
            if idx > last_line_idx:
                break
            if idx >= first_line_idx:
                fout.write(src_line)
                if first_idx_written is None:
                    first_idx_written = idx
                last_idx_written = idx
        print("Done. Written lines {} to {} to file: {}".format(
            first_idx_written if first_idx_written is not None else "n/a",
            last_idx_written if last_idx_written is not None else "n/a",
            dest_path
        ))
