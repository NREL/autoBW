"""Utility functions."""

from hashlib import md5

from bw2data.backends.peewee import proxies


def get_hash(*kvals) -> str:
    """
    Compute MD5 sum.

    :param kvals: object to compute over
    :return: MD5 hash
    """
    text = " ".join([str(kval) for kval in kvals])

    return md5(text.encode("utf-8")).hexdigest()


def validate_activity(activity: proxies.Activity) -> None:
    """
    Check for 'name', 'location', 'type', 'unit', 'version', 'comment' attributes in <activity>.

    :param activity: brightway activity or exchange
    :return:
    """
    key = activity.key[1]

    for attribute in ["name", "location", "type", "unit", "version"]:
        try:
            assert activity[attribute] is not None
        except KeyError as missing_attribute:
            raise AttributeError(
                f"attribute {attribute} is missing for activity {key}"
            ) from missing_attribute
        except AssertionError as null_attribute:
            raise AttributeError(
                f"attribute {attribute} is Null in activity: {key}"
            ) from null_attribute
