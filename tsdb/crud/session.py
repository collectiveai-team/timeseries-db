"""
This module is now deprecated. Session management is handled by the connectors.
"""


class DeprecatedCRUDSession:
    """
    This class is deprecated and should not be used.
    Session management is now handled within each connector.
    """

    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "CRUDSession is deprecated. Database connections and sessions are now managed by connectors."
        )
