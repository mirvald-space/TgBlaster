"""
TgBlaster Handlers Module

Organized handlers for Telegram bot functionality:
- account: Account management (add, delete, view)
- group: Group management (add, delete, view, info)
- broadcast: Broadcasting functionality (start, manage, all accounts)
- admin: Admin functionality (start, history)
"""

# Import all handlers from organized modules
from .account import *
from .group import *
from .broadcast import *
from .admin import *
