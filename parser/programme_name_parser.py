import re
from collections import Counter
from typing import Dict, Iterable, Optional


class ProgrammeNameParser:
    def try_extract_programme_name(self, title: str) -> Optional[str]:
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

        title = _normalize_spaces(title)
        title = _remove_date(title)
        title = title.strip()
        try_patterns = [
            # highest precedence
            r'^《(.*?)》',
            r'^【(.*)】',
            r'^「(.*)」',
            r'(.*?)：',
            r'(.*?) – ',
            r'(.*?):',
            r'(.*?)-',
            r'(.*?)_',
            r'(.*?) ',
            # lowest precedence
        ]
        for pattern in try_patterns:
            match = re.search(pattern, title)
            if match:
                return match.group(1).strip()
        return None

    def collect_programme_names_with_counts(self, titles: Iterable[str]) -> Dict[str, int]:
        # This is on a best guess basis -
        #   we assume common substrings (i.e. substrings that appear more than once among the titles)
        #   are programme names.
        programme_name_counter: Counter[str] = Counter()
        for title in titles:
            programme_name = self.try_extract_programme_name(title)
            if programme_name:
                programme_name_counter[programme_name] += 1

        programme_names_with_counts = {programme_name: count
                                       for programme_name, count
                                       in programme_name_counter.most_common()
                                       if count > 1}

        return programme_names_with_counts
