from motor.metaprogramming import AsyncRead
from motor.core import AgnosticCursor

setattr(AgnosticCursor, 'count', AsyncRead())
