import os

from tracardi.config import tracardi
from tracardi.service.utils.environment import get_env_as_int


class Config:
    def __init__(self, env):
        self.tenant = env.get('TENANT', tracardi.version.name)
        self.mode = env.get('MODE', 'job')
        self.pause = get_env_as_int('PAUSE', 60)


config = Config(os.environ)
