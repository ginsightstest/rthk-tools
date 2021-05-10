def longest_common_prefix(s1: str, s2: str) -> str:
    """
    >>> [longest_common_prefix(*pair) for pair in [('', ''), ('abc', 'abde'), ('abc', 'acde'), ('abc', 'bcd')]]
    ['', 'ab', 'a', '']
    """
    common_prefix = ''
    for (c1, c2) in zip(s1, s2):
        if c1 == c2:
            common_prefix += c1
        else:
            return common_prefix
    return common_prefix


if __name__ == "__main__":
    import doctest

    doctest.testmod()
