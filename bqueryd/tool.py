import errno
import os
import shutil


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def rm_file_or_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            if os.path.islink(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
        else:
            if os.path.islink(path):
                os.unlink(path)
            else:
                os.remove(path)
