import re
from collections import Counter
from typing import Dict, List, Optional

from util.strings import longest_common_prefix


class ProgrammeNameParser:
    def _try_extract_programme_name(self, title: str) -> Optional[str]:
        try_patterns = [
            # highest precedence
            r'(.*?)：',
            r'(.*?):',
            r'^《(.*?)》',
            r'^【(.*)】',
            r'^「(.*)」',
            r'(.*?) – ',
            r'(.*?)-',
            r'(.*?)_',
            # lowest precedence
        ]
        for pattern in try_patterns:
            match = re.search(pattern, title)
            if match:
                return match.group(1).strip()
        return None

    def _try_extract_common_programme_name(self, title1: str, title2: str) -> Optional[str]:
        match = longest_common_prefix(title1, title2)
        if match and not match[0].islower():
            # If programme name is english, check that it starts with uppercase
            return match
        return None

    def _strip_down_title(self, title: str) -> str:
        def _normalize_spaces(title: str) -> str:
            return re.sub(r'\s+', ' ', title)

        def _remove_date(title: str) -> str:
            for pattern in (
                    r'\d{1,2}[-_./ ]\d{1,2}[-_./ ]\d{4}',
                    r'\d{4}[-_./ ]\d{1,2}[-_./ ]\d{1,2}',
                    r'\d{1,2}[-_./ ]\d{1,2}[-_./ ]\d{2}',
                    r'\d{2}[-_./ ]\d{1,2}[-_./ ]\d{1,2}',
                    r'\S+月\S+日',
            ):
                title = re.sub(pattern, '', title)
            return title

        def _remove_episode(title: str) -> str:
            return re.sub(r'第\s*([^第]+?)\s*集', '', title)

        def _remove_surrounding_punctuation(title: str) -> str:
            return title.strip('：–:-_ ')

        title = _normalize_spaces(title)
        title = _remove_date(title)
        title = _remove_episode(title)
        title = _remove_surrounding_punctuation(title)
        return title

    def collect_programme_names_with_counts(self, titles: List[str]) -> Dict[str, int]:
        # This is on a best guess basis -
        #   we assume common substrings (i.e. substrings that appear more than once among the titles)
        #   are programme names.
        programme_name_counter: Counter[str] = Counter()
        titles_count = len(titles)
        sorted_titles = sorted(map(self._strip_down_title, titles))
        for index, title in enumerate(sorted_titles):
            programme_name = self._try_extract_programme_name(title)
            if not programme_name and (index + 1) < titles_count:
                next_title = titles[index + 1]
                programme_name = self._try_extract_common_programme_name(title, next_title)
            if programme_name:
                programme_name_counter[programme_name] += 1

        programme_names_with_counts = {programme_name: count
                                       for programme_name, count
                                       in programme_name_counter.most_common()
                                       if count > 1}

        return programme_names_with_counts
