# -*- coding: utf-8 -*-
from collections import OrderedDict, namedtuple
from itertools import chain, tee
from pathlib import Path
from typing import FrozenSet, List, NamedTuple, Optional, Set, Tuple, TypeVar, Union

import attr
from airflow.models import Variable

TRIGGER_TYPES = ("upsert", "delete", "insert", "update")


class Pipeline(NamedTuple):
    path: str = None
    trigger_path: str = None
    dependencies: List[str] = None


def get_sql_dir():
    sql_dir = Path(Variable.get("sql_dir"))
    if not sql_dir.exists():
        PKG_PARENT = Path(__file__).absolute().parent.parent.parent.parent
        sql_dir = PKG_PARENT / "airflow-core/sql"
    return sql_dir


def convert_path(path: Optional[Path]) -> Optional[Path]:
    if not path:
        return None
    if not isinstance(path, Path):
        path = Path(path)
    if not path.is_absolute():
        path = SqlFile.DEFAULT_SQL_DIR / path
    return path


def dedup(iterable):
    return iter(OrderedDict.fromkeys(iterable))


class SqlFile__MetaClass(type):
    @property
    def DEFAULT_SQL_DIR(cls):
        sql_dir = Path(Variable.get("sql_dir"))
        if not sql_dir.exists():
            PKG_PARENT = Path(__file__).absolute().parent.parent.parent.parent
            sql_dir = PKG_PARENT / "airflow-core/sql"
        return sql_dir / "salesforce"


@attr.s(frozen=True)
class SqlFile(metaclass=SqlFile__MetaClass):
    path = attr.ib(type=Path, converter=convert_path)
    sql = attr.ib(type=str, default=None)
    dependencies = attr.ib(factory=tuple, hash=False, type=Tuple["SqlFile"])
    dependants = attr.ib(factory=tuple, hash=False, type=Tuple["SqlFile"])
    triggers = attr.ib(factory=tuple, hash=False, type=Tuple["SqlFile"])

    @property
    def name(self) -> str:
        if self.path.parent.name == "salesforce":
            return self.path.stem
        return f"{self.path.parent.name}/{self.path.stem}"

    @property
    def is_trigger(self):
        """Indicates whether the current sql file represents a trigger

        Triggers are the direct result of an upstream database execution, e.g. every time
        an insert is performed, the given trigger is then performed.  In this context, a
        trigger simply represents any sql that should be invoked as a result of another
        sql file running.

        :return: Whether the current file is a trigger
        :rtype: bool
        """
        return any(self.name.startswith(trigger) for trigger in TRIGGER_TYPES)

    def merge(self, other: "SqlFile") -> "SqlFile":
        """Merges the given dependency tree with another metadata set for the same file

        This is typically used for updating the dependency and trigger information of a
        given sql file instance.

        :return: A new instance of the sql file with the given instance merged in
        :rtype: SqlFile
        """
        new_dependencies = tuple(self.dependencies) + other.dependants
        new_triggers = tuple(self.triggers) + other.triggers
        new_dependants = tuple(self.dependants) + other.dependants
        new_dependants = tuple(list(dedup(new_dependants)))
        new_dependencies = tuple(list(dedup(new_dependencies)))
        new_triggers = tuple(list(dedup(new_triggers)))
        return attr.evolve(
            self,
            dependencies=new_dependencies,
            dependants=new_dependants,
            triggers=new_triggers,
        )

    def load_sql(self):
        return attr.evolve(self, sql=self.path.read_text())

    @classmethod
    def merge_and_update(
        cls, original_list: List["SqlFile"], new_file: "SqlFile"
    ) -> List["SqlFile"]:
        if new_file in original_list:
            idx = original_list.index(new_file)
            new_file = new_file.merge(original_list.pop(idx))
            original_list.insert(idx, new_file)
        else:
            original_list.append(new_file)
        return original_list

    @classmethod
    def merge_from_list(
        cls, original_list: List["SqlFile"], new_list: List["SqlFile"]
    ) -> List["SqlFile"]:
        for sql_file in new_list:
            original_list = cls.merge_and_update(original_list, sql_file)
        return original_list

    def depends_on(self, sql_file: "SqlFile") -> "SqlFile":
        """Indicate that the current sql file has an upstream dependency on *sql_file*

        This tells the task runner that before this file can be executed, *sql_file* must
        be executed succesfully.

        :param SqlFile sql_file: A :class:`SqlFile` instance which must run first
        :return: An updated version of the current :class:`SqlFile` with new dependencies
        :rtype: SqlFile
        """
        dep_list: List["SqlFile"] = list(self.dependencies)
        new_deps = tuple(self.merge_and_update(dep_list, sql_file))
        return attr.evolve(self, dependencies=new_deps)

    def with_dependencies(self, dependencies: List["SqlFile"]) -> "SqlFile":
        """Indicate that the current sql file has multiple upstream dependencies.

        This tells the task runner that before this file can be executed, *dependencies*
        must all be executed successfully.

        :return: An updated version of the current :class:`SqlFile` with new dependencies
        :rtype: SqlFile
        """
        dep_list: List["SqlFile"] = list(self.dependencies)
        new_dependencies = tuple(self.merge_from_list(dep_list, dependencies))
        return attr.evolve(self, dependencies=new_dependencies)

    def with_trigger(self, sql_file: "SqlFile") -> "SqlFile":
        """Indicate that the current sql file triggers *sql_file* to run

        This tells the task runner that before after this file is executed, *sql_file*
        should be executed.

        :param SqlFile sql_file: A :class:`SqlFile` instance which must run first
        :return: An updated version of the current :class:`SqlFile` with new triggers
        :rtype: SqlFile
        """
        trigger_list: List["SqlFile"] = list(self.triggers)
        new_triggers = tuple(self.merge_and_update(trigger_list, sql_file))
        return attr.evolve(self, triggers=new_triggers)

    def with_triggers(self, triggers: List["SqlFile"]) -> "SqlFile":
        """Indicate that the current sql file has multiple downstream triggers.

        This tells the task runner that after this file is executed, *triggers*
        must all be executed.

        :return: An updated version of the current :class:`SqlFile` with new triggers
        :rtype: SqlFile
        """
        trigger_list: List["SqlFile"] = list(self.triggers)
        new_triggers = tuple(self.merge_from_list(trigger_list, triggers))
        return attr.evolve(self, triggers=new_triggers)

    def with_child(self, sql_file: "SqlFile") -> "SqlFile":
        """Indicate that the current sql file has a downstream dependant of *sql_file*

        This tells the task runner that after this file is executed, *sql_file* should
        be executed.

        :param SqlFile sql_file: A :class:`SqlFile` instance which waits on this file
        :return: An updated version of the current :class:`SqlFile` with new children
        :rtype: SqlFile
        """
        dependant_list: List["SqlFile"] = list(self.dependants)
        new_dependants = tuple(self.merge_and_update(dependant_list, sql_file))
        return attr.evolve(self, dependants=new_dependants)

    def with_children(self, children: List["SqlFile"]) -> "SqlFile":
        """Indicate that the current sql file has multiple downstream child dependants.

        This tells the task runner that after this file is executed, *dependencies*
        should all be executed.

        :return: An updated version of the current :class:`SqlFile` with new dependants
        :rtype: SqlFile
        """
        dependant_list: List["SqlFile"] = list(self.dependants)
        new_dependants = tuple(self.merge_from_list(dependant_list, children))
        return attr.evolve(self, dependants=new_dependants)

    @classmethod
    def from_tuple(
        cls,
        pipeline: Tuple[
            Union[str, Path],
            Union[str, List[Union[str, "SqlFile"]]],
            List[Union[str, "SqlFile"]],
        ],
    ) -> Tuple["SqlFile", List["SqlFile"], Optional[List["SqlFile"]]]:
        """Creates a :class:`SqlFile` instance from a tuple of file paths.

        :return: A new :class:`SqlFile` and its corresponding triggers and trigger deps
        :rtype: Tuple[`SqlFile`, List[`SqlFile`], Optional[List[`SqlFile`]]]
        """
        path, triggers, deps = pipeline
        trigger_list: List[Union[str, "SqlFile"]] = []
        if triggers and not isinstance(triggers, (list, tuple)):
            trigger_list = [triggers]
        elif triggers:
            trigger_list = list(triggers)
        return cls.create(path=path, triggers=trigger_list, dependencies=deps)

    @classmethod
    def build_trigger(
        cls,
        parent: "SqlFile",
        trigger: Union["SqlFile", str],
        deps: Optional[List["SqlFile"]],
    ) -> Tuple["SqlFile", "SqlFile", Optional[List["SqlFile"]]]:
        if isinstance(trigger, cls):
            trigger_instance: "SqlFile" = trigger
        elif isinstance(trigger, str):
            if not trigger.endswith(".sql"):
                trigger = f"{trigger}.sql"
            trigger_instance: "SqlFile" = cls(path=trigger).load_sql()  # type: ignore
        if deps:
            dep_list = [dep.with_child(trigger_instance) for dep in deps]
            trigger_instance = trigger_instance.with_dependencies(deps)
            return (
                parent.with_trigger(trigger_instance.with_dependencies(deps)),
                trigger_instance,
                dep_list,
            )
        return (parent.with_trigger(trigger_instance), trigger_instance, [])

    @classmethod
    def build_dependencies(
        cls, dependencies: List[Union["SqlFile", str]], parent: Optional["SqlFile"] = None
    ) -> Tuple[Optional["SqlFile"], List["SqlFile"]]:
        dep_instances: List["SqlFile"] = []
        if not dependencies:
            return parent, []
        for dep in dependencies:
            if not dep:
                continue
            if isinstance(dep, str):
                if not dep.endswith(".sql"):
                    dep = f"{dep}.sql"
                dep_instance: "SqlFile" = cls(path=dep).load_sql()  # type: ignore
            elif isinstance(dep, cls):
                dep_instance = dep
            if dep not in dep_instances:
                dep_instances.append(dep_instance)
        if parent:
            return parent.with_dependencies(dep_instances), dep_instances
        return parent, dep_instances

    @classmethod
    def build_triggers(
        cls,
        parent: "SqlFile",
        triggers: List[Union[str, "SqlFile"]],
        dependencies: List[Union[str, "SqlFile"]],
    ) -> Tuple["SqlFile", List["SqlFile"], Optional[List["SqlFile"]]]:
        deps: Optional[List["SqlFile"]] = None
        _, deps = cls.build_dependencies(dependencies)
        trigger_instances: List["SqlFile"] = []
        for trigger in triggers:
            if not trigger:
                continue
            parent, trigger, deps = cls.build_trigger(parent, trigger, deps)
            trigger_instances.append(trigger.depends_on(parent))
        return parent, trigger_instances, deps

    @classmethod
    def create(
        cls,
        path: Union[str, Path],
        triggers: List[Union[str, "SqlFile"]] = None,
        dependencies: List[Union[str, "SqlFile"]] = None,
    ) -> Tuple["SqlFile", List["SqlFile"], Optional[List["SqlFile"]]]:
        """Given a path and a list of triggered sql files, produces a SqlFile + triggers

        :param Union[str, Path] path: Path to the parent file
        :param List[Union[str, SqlFile]] triggers: A list of sql triggered by the parent
        :param List[Union[str, SqlFile]] dependencies: A list of trigger dependency files
        :return: A 2-tuple representing the parent sqlfile and its dependant triggers
        :rtype: Tuple[SqlFile, List[SqlFile], Optional[List[SqlFile]]]
        """
        if triggers is None:
            triggers = []
        if dependencies is None:
            dependencies = []
        if not path:
            raise TypeError(f"Expected an value for *path*")
        if not isinstance(path, Path):
            path_sqlfile = cls(path=Path(path)).load_sql()
        else:
            path_sqlfile = cls(path=path).load_sql()
        return cls.build_triggers(path_sqlfile, triggers, dependencies)


def get_upsert_mapping(pipeline_list: List[Pipeline], allow_upsert: bool = False):
    """Given a list of **pipeline** instances, create dependency graphs and return them.

    :param pipeline_list: A list of `namedtuple` instances with (file, trigger, deps)
    :type pipeline_list: List[`namedtuple`]
    :param allow_upsert: Whether to include `deps` in the upsert tree, defaults to False
    :param allow_upsert: bool, optional
    :return: A mapping of names to `SqlFile` instances, list of roots, list of triggers
    :rtype: Tuple[Dict[str, SqlFile], List[SqlFile], List[SqlFile]]
    """
    UPSERT_PATH = {}
    UPSERT_SEQUENCE = []
    TRIGGERS = []
    for pipeline in pipeline_list:
        insert, triggers, dependencies = SqlFile.from_tuple(pipeline)
        if not allow_upsert:
            triggers = []
        for trigger in triggers:
            path_value = UPSERT_PATH.get(trigger.name, {})
            existing_file = path_value.get("sql_file", None)
            if not existing_file:
                UPSERT_PATH[trigger.name] = {"sql_file": trigger}
            else:
                UPSERT_PATH[trigger.name]["sql_file"] = trigger.merge(existing_file)
            TRIGGERS.append(UPSERT_PATH[trigger.name]["sql_file"])
        UPSERT_PATH[insert.name] = {"sql_file": insert}
        UPSERT_SEQUENCE.append(insert)
    return UPSERT_PATH, UPSERT_SEQUENCE, TRIGGERS


def pairwise(seq):
    a, b = tee(seq)
    next(b, None)
    return zip(a, b)


def annotated_last(seq):
    """Returns an iterable of pairs of input item and a boolean that show if
    the current item is the last item in the sequence."""
    MISSING = object()
    for current_item, next_item in pairwise(chain(seq, [MISSING])):
        yield current_item, next_item is MISSING
