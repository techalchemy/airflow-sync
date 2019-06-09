# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan
import pathlib

import invoke
import parver

PACKAGE_NAME = "airflow_sync"

ROOT = pathlib.Path(__file__).resolve().parent.parent


@invoke.task()
def typecheck(ctx):
    src_dir = ROOT / "src" / PACKAGE_NAME
    src_dir = src_dir.as_posix()
    env = {"MYPYPATH": src_dir}
    ctx.run(f"mypy {src_dir}", env=env)


@invoke.task
def build_docs(ctx):
    from airflow_sync import __version__

    _current_version = parver.Version.parse(__version__)
    minor = [str(i) for i in _current_version.release[:2]]
    docs_folder = (ROOT / "docs").as_posix()
    if not docs_folder.endswith("/"):
        docs_folder = "{0}/".format(docs_folder)
    args = ["--ext-autodoc", "--ext-viewcode", "-o", docs_folder]
    args.extend(["-A", "'Dan Ryan <dan.ryan@xyleminc.com>'"])
    args.extend(["-R", str(_current_version)])

    args.extend(["-V", ".".join(minor)])
    args.extend(["-e", "-M", "-F", f"src/{PACKAGE_NAME}"])
    print("Building docs...")
    ctx.run("sphinx-apidoc {0}".format(" ".join(args)))


ns = invoke.Collection(typecheck)
