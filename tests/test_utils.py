def dict_equals(dict1: dict, dict2: dict) -> bool:
    """A helper function to compare two dictionaries for equality.

    Args:
        dict1 (dict): The first dictionary to compare.
        dict2 (dict): The second dictionary to compare with.

    Returns:
        bool: True if the dictionaries are equal, False otherwise.

    """
    #     From Python version 3.6+, dictionary comparison happens agnostic of the order of the keys.
    #     See [official documentation](https://docs.python.org/2/reference/expressions.html#not-in) for more details.
    return dict1 == dict2
