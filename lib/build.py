import os
import shutil
from setuptools.command.build_ext import build_ext
from setuptools.dist import Distribution
from setuptools import Extension

from Cython.Build import cythonize

compile_args = ["-O3"]
link_args = []
include_dirs = []
libraries = ["m"]

def build():
    extensions = [
        Extension(
            "*",
            ["asyncreflex/*.pyx"],
            extra_compile_args=compile_args,
            extra_link_args=link_args,
            include_dirs=include_dirs,
            libraries=libraries,
        )
    ]
    ext_modules = cythonize(
        extensions,
        include_path=include_dirs,
        compiler_directives={"binding": True, "language_level": 3},
        force=True,
    )

    distribution = Distribution({
        "name": "asyncreflex",
        "ext_modules": ext_modules,
        "package_dir": {"asyncreflex": "asyncreflex"}
    })

    cmd = build_ext(distribution)
    cmd.ensure_finalized()
    cmd.run()

    # Copy built extensions back to the project
    for output in cmd.get_outputs():
        relative_extension = os.path.relpath(output, cmd.build_lib)
        shutil.copyfile(output, relative_extension)
        mode = os.stat(relative_extension).st_mode
        mode |= (mode & 0o444) >> 2
        os.chmod(relative_extension, mode)

if __name__ == "__main__":
    build()
