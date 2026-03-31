def evaluate_not_null(value):
    return value is not None


def test_not_null_rule():
    assert evaluate_not_null("abc") is True
    assert evaluate_not_null(None) is False
