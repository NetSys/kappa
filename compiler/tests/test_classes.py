import re

import rt


class Text(object):
    def __init__(self, text):
        self.text = text
        print("Pausing inside __init__().")
        rt.pause()
        self.pattern = re.compile(r"\w+")

    def compute_number_of_words(self, _unused):
        # The unused param is used to force pickling of this method.
        words = re.findall(self.pattern, self.text)
        rt.pause()
        return len(words)


def make_bool():
    rt.pause()
    print("I feel like pausing.")
    return True


def test(raw_text):
    raw_text = raw_text.strip()
    rt.pause()
    text = Text(raw_text)
    print("The text you entered is: {}".format(raw_text))
    num_words = text.compute_number_of_words(make_bool())
    print("The text has {} words.".format(num_words))
    return num_words


def handler(event, context):
    return test(event["raw_text"])
