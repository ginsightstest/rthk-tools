from typing import Iterable, List, TypeVar

T = TypeVar('T')


def flatten(l: Iterable[List[T]]) -> List[T]:
    """
    >>> flatten(([1, 2, 3], [4, 5], [], [6]))
    [1, 2, 3, 4, 5, 6]
    >>> flatten([[1, 2, 3], [4, 5], [], [6]])
    [1, 2, 3, 4, 5, 6]
    """
    return sum(l, [])


if __name__ == "__main__":
    import doctest

    doctest.testmod()
