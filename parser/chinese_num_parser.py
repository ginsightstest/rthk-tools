import re

_SINGLE_DIGIT_MAPPING = {
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10
}


class ChineseNumParser:
    def parse(self, chinese_num_string: str) -> int:
        """
        >>> [p.parse(n) for n in ["", "一", "十一", "二十", "二十一", "廿一", "一百", "一百零一", "一百一十", "一百一十一"]]
        [0, 1, 11, 20, 21, 21, 100, 101, 110, 111]
        """
        num = 0
        chinese_num_string = chinese_num_string.replace('零', '')
        match = re.fullmatch(r'(\S+)百(\S*)', chinese_num_string)
        if match:
            chinese_hundred_digit, chinese_num_string = match.groups()
            num += self._parse_single_digit(chinese_hundred_digit) * 100

        match = re.fullmatch(r'廿(\S+)', chinese_num_string)
        if match:
            chinese_num_string = match.group(1)
            num += 20

        match = re.fullmatch(r'(\S*)十(\S*)', chinese_num_string)
        if match:
            chinese_tens_digit, chinese_num_string = match.groups()
            if chinese_tens_digit:
                num += self._parse_single_digit(chinese_tens_digit) * 10
            else:
                num += 10

        if chinese_num_string:
            num += self._parse_single_digit(chinese_num_string)

        return num

    def _parse_single_digit(self, chinese_digit: str) -> int:
        return _SINGLE_DIGIT_MAPPING[chinese_digit]


if __name__ == "__main__":
    import doctest

    doctest.testmod(extraglobs={'p': ChineseNumParser()})
