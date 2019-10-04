import os, sys

def add_path(path):
    if path not in sys.path:
        sys.path.insert(0, path)

this_dir = os.path.dirname(os.path.realpath(__file__))
#add_path(os.path.join(this_dir, "..", "tools"))
#add_path(os.path.join(this_dir, ".."))