

def xnor(a: bool, b: bool) -> bool:
    """
    Returns XNOR of two conditions.

    Convenience function to make the nature of comparison more obvious. Compare e. g.:
    ```
    if a is None == b is None:
        ...
    ```
    vs.
    ```
    if xnor(a is None, b is None):
        ...
    ```
    """
    return a == b