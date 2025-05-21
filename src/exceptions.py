class BackendConnectionException(Exception):
    pass


class TransactionInterrupted(Exception):
    pass


class DependencyException(Exception):
    pass


class DependencyBrokenException(DependencyException):
    pass
