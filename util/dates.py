def duration_to_seconds(duration_str: str) -> int:
    """
    >>> duration_to_seconds('00:42:44')
    2564
    """
    h, m, s = [int(i) for i in duration_str.split(':')]
    return 3600 * h + 60 * m + s


if __name__ == "__main__":
    import doctest

    doctest.testmod()
