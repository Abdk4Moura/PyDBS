import random
import string


def grs(with_letters: bool = True, with_numbers: bool = False, with_symbols: bool = False, length=8):
    random_pattern = ""
    if with_letters:
        random_pattern += string.ascii_letters
    if with_numbers:
        random_pattern += string.digits
    if with_symbols:
        random_pattern += string.punctuation

    return ''.join([random.choice(random_pattern) for _ in
                    range(length)])
