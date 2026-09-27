"""Text-quality measures shared by the re-OCR and comparison scripts."""
import re
from pathlib import Path

# macOS system word list (Webster's Second, 235,976 words) plus planning terms it lacks.
DICTIONARY = {w.strip().lower() for w in Path('/usr/share/dict/words').read_text().split()} | {
    'rezoning', 'rezone', 'rezoned', 'ulurp', 'cpc', 'ceqr', 'feis', 'deis', 'eis', 'dcp', 'hpd', 'dos', 'dot',
    'far', 'mih', 'uap', 'udaap', 'bsa', 'lpc', 'mta', 'dep', 'dpr', 'dsny', 'edc', 'nycha', 'bp', 'cb',
    'nyc', 'bronx', 'brooklyn', 'queens', 'manhattan', 'staten'}
WORD = re.compile(r'[A-Za-z]{2,}')


def dictionary_share(text):
    """Share of alphabetic tokens (2+ letters) that are dictionary words: a rough garble rate."""
    words = WORD.findall(text)
    if not words:
        return None
    return sum(w.lower() in DICTIONARY or w.lower().rstrip('s') in DICTIONARY for w in words) / len(words)


def alphabetic_words(text):
    return len(WORD.findall(text))
