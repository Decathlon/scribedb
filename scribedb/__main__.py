"""Allow moulinette to be executable through `python -m moulinette`."""
from .scribedb import scribedb  # pragma: no cover

if __name__ == "__main__":  # pragma: no cover
    scribedb()
