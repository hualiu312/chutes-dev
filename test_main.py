import asyncio
import sys
import traceback
from loguru import logger

sys.path.append("..")

from gepetto import RemoteApi

print(RemoteApi.__dict__)
