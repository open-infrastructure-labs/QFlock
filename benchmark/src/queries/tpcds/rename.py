import os
import shutil
import re

for f in os.listdir():
    new_name = re.sub("^q", "", f)
    if new_name != f:
        print(f"{f} -> {new_name}")
        shutil.copy(f,new_name)
