from collections import namedtuple
from datetime import datetime, timedelta
from operator import itemgetter
from typing import Generator, Iterable, List, Optional, Union
from AbstractDataHandler import BaseDataHandler


class CHDataHandler(BaseDataHandler):
    def load_data(self):
        return