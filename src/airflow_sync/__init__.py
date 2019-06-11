# -*- coding=utf-8 -*-
import inspect
import sys
from typing import List

import airflow.executors
import airflow.hooks
import airflow.operators
import airflow.plugins_manager
import airflow.sensors
import pkg_resources
from airflow.utils.log.logging_mixin import LoggingMixin

__version__ = "0.0.3"
log = LoggingMixin().log


PATCHED_AIRFLOW = False


# * Until we are running airflow 1.10.2 in production we will need to patch this
# * in order to enable importing entry-point based plugins
def load_entrypoint_plugins(entry_points, airflow_plugins):
    """
    Load AirflowPlugin subclasses from the entrypoints
    provided. The entry_point group should be 'airflow.plugins'.
    :param entry_points: A collection of entrypoints to search for plugins
    :type entry_points: Generator[setuptools.EntryPoint, None, None]
    :param airflow_plugins: A collection of existing airflow plugins to
        ensure we don't load duplicates
    :type airflow_plugins: list[type[airflow.plugins_manager.AirflowPlugin]]
    :rtype: list[airflow.plugins_manager.AirflowPlugin]
    """
    for entry_point in entry_points:
        log.debug("Importing entry_point plugin %s", entry_point.name)
        plugin_obj = entry_point.load()
        if is_valid_plugin(plugin_obj, airflow_plugins):
            if callable(getattr(plugin_obj, "on_load", None)):
                plugin_obj.on_load()
            airflow_plugins.append(plugin_obj)
    return airflow_plugins


def is_valid_plugin(plugin_obj, existing_plugins):
    """
    Check whether a potential object is a subclass of
    the AirflowPlugin class.
    :param plugin_obj: potential subclass of AirflowPlugin
    :param existing_plugins: Existing list of AirflowPlugin subclasses
    :return: Whether or not the obj is a valid subclass of
        AirflowPlugin
    """
    if (
        inspect.isclass(plugin_obj)
        and issubclass(plugin_obj, airflow.plugins_manager.AirflowPlugin)
        and (plugin_obj is not airflow.plugins_manager.AirflowPlugin)
    ):
        plugin_obj.validate()
        return plugin_obj not in existing_plugins
    return False


def patch_airflow():
    global PATCHED_AIRFLOW
    if PATCHED_AIRFLOW:
        return
    plugins: List[airflow.plugins_manager.AirflowPlugin] = airflow.plugins_manager.plugins

    plugins = load_entrypoint_plugins(
        pkg_resources.iter_entry_points("airflow.plugins"), plugins
    )
    airflow.plugins_manager.plugins = plugins

    for p in plugins:
        airflow.plugins_manager.operators_modules.append(
            airflow.plugins_manager.make_module(
                "airflow.operators." + p.name, p.operators + p.sensors
            )
        )
        airflow.plugins_manager.sensors_modules.append(
            airflow.plugins_manager.make_module("airflow.sensors." + p.name, p.sensors)
        )
        airflow.plugins_manager.hooks_modules.append(
            airflow.plugins_manager.make_module("airflow.hooks." + p.name, p.hooks)
        )
        airflow.plugins_manager.executors_modules.append(
            airflow.plugins_manager.make_module(
                "airflow.executors." + p.name, p.executors
            )
        )
        airflow.plugins_manager.macros_modules.append(
            airflow.plugins_manager.make_module("airflow.macros." + p.name, p.macros)
        )

        airflow.plugins_manager.admin_views.extend(p.admin_views)
        airflow.plugins_manager.menu_links.extend(p.menu_links)
        airflow.plugins_manager.flask_appbuilder_views.extend(p.appbuilder_views)
        airflow.plugins_manager.flask_appbuilder_menu_links.extend(
            p.appbuilder_menu_items
        )
        airflow.plugins_manager.flask_blueprints.extend(
            [{"name": p.name, "blueprint": bp} for bp in p.flask_blueprints]
        )

    sys.modules["airflow.plugins_manager"] = airflow.plugins_manager
    airflow.hooks._parent_module._integrate_plugins()
    sys.modules["airflow.hooks"] = airflow.hooks
    airflow.operators._parent_module._integrate_plugins()
    sys.modules["airflow.operators"] = airflow.operators
    airflow.executors._integrate_plugins()
    sys.modules["airflow.executors"] = airflow.executors
    airflow.sensors._parent_module._integrate_plugins()
    sys.modules["airflow.sensors"] = airflow.sensors
    PATCHED_AIRFLOW = True


def apply_patch():
    if not PATCHED_AIRFLOW:
        patch_airflow()


apply_patch()
