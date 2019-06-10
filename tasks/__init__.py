# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan
import os
import pathlib
import shutil

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


@invoke.task()
def build_package(ctx):
    dist_dir = ROOT / "dist"
    build_dir = ROOT / "build"
    if dist_dir.exists():
        shutil.rmtree(dist_dir.as_posix())
    if build_dir.exists():
        shutil.rmtree(build_dir.as_posix())
    ctx.run("pip install -e . --no-use-pep517")
    ctx.run("python setup.py sdist bdist_wheel")
    ctx.run("twine check dist/*")


@invoke.task()
def upload(ctx):
    password = os.environ.get("AZURE_UPLOAD_PASSWORD")
    twine_upload_url = os.environ.get("AZURE_UPLOAD_URL")
    username = os.environ.get("AZURE_FEED_NAME")
    env = {"TWINE_USERNAME": username, "TWINE_PASSWORD": password}
    ctx.run(f"twine upload dist/* --repository-url={twine_upload_url}", env=env)


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
