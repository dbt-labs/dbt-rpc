
import os, shutil

ROOT_PATH = "./working-dir"

def get_root_path(state_id):
    return os.path.join(ROOT_PATH, f"state-{state_id}")

def get_path(state_id, filename):
    return os.path.join(get_root_path(state_id), filename)

def write_file(path, contents):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(path, 'wb') as fh:
        fh.write(contents.encode('utf-8'))


def write_unparsed_manifest_to_disk(state_id, filedict):
    root_path = get_root_path(state_id)
    if os.path.exists(root_path):
        shutil.rmtree(root_path)

    for filename, body in filedict.items():
        path = get_path(state_id, filename)
        write_file(path, body['contents'])
