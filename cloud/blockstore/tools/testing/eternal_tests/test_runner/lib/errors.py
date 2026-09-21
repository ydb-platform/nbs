class Error(Exception):
    pass


# A test VM absent from a successfully retrieved instance list.
class InstanceNotFoundError(Error):
    pass
