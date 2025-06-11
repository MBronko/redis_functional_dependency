class DatabaseException(Exception):
    pass


class BackendConnectionException(DatabaseException):
    pass


class InvalidDescriptorException(DatabaseException):
    pass


class TransactionInterrupted(DatabaseException):
    pass


class DependencyException(DatabaseException):
    pass


class DependencyBrokenException(DependencyException):
    pass
