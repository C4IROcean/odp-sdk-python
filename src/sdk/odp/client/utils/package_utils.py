from importlib.metadata import version


def get_version():
    try:
        return str(version("odp-sdk"))
    except Exception as e:
        print(e)
        return ""
