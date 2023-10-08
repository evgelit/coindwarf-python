import json
from pathlib import Path

env_path = Path(__file__).with_name('env.json')
env_path = env_path.absolute()
env = open(env_path)
env = json.load(env)
