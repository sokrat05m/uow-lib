class UoWError(Exception):
    pass


class UnregisteredEntityError(UoWError):
    pass


class DuplicateEntityError(UoWError):
    pass


class UntrackedEntityError(UoWError):
    pass


class CyclicDependencyError(UoWError):
    pass
