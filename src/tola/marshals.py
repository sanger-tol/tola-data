import datetime
import logging
import os
import pwd
import re
from functools import cache, cached_property

import click
import pytz
from sqlalchemy import Integer, String, select
from sqlalchemy.orm import mapped_column
from tol.api_client import ApiDataSource, ApiObject
from tol.core import DataSourceFilter
from tol.sql.sql_converter import DefaultDataObjectConverter
from tolqc import models_list
from tolqc.model import Base, LogBase
from tolqc.sample_data_models import Data, File, Project

from tola import db_connection


class User(Base):
    __tablename__ = "user"

    @classmethod
    def get_id_column_name(cls):
        return "accession_type_id"

    id = mapped_column(Integer, primary_key=True)  # noqa A003
    name = mapped_column(String, nullable=False)
    email = mapped_column(String, nullable=False, unique=True)
    organisation = mapped_column(String)


class MarshalParamType(click.ParamType):
    name = "marshal-type"

    def convert(self, value, param, ctx):
        if m := re.search(r"^(sql|api)(?::(.+))?$", value):
            cls, db_alias = m.groups()
        else:
            msg = f"Unknown Marshal type '{value}'; Valid types: api | sql[:db_alias]"
            raise ValueError(msg)

        try:
            if cls == "sql":
                return TolSqlMarshal(db_alias)
            elif cls == "api":
                return TolApiMarshal()
        except ValueError as ve:
            self.fail(ve)


mrshl = click.option(
    "--mrshl",
    "--marshal-type",
    "mrshl",
    type=MarshalParamType(),
    envvar="TOLQC_MARSHAL",
    default="sql",
    show_default=True,
    help=(
        "Valid marshal types are 'api' to connect via the ToL http API,"
        " or 'sql[:db_alias]' to connect via SQLAlchemy with optional"
        " db_alias parameter. Can also be set via the TOLQC_MARSHAL"
        " environment variable."
    ),
)


class TolBaseMarshal:
    def build_filter(self, cls, spec, selector=None):
        # Use the primary key if there is no selector
        if not selector:
            selector = (cls.get_id_column_name(),)

        # Build a filter for the select query
        fltr = {}
        for sel in selector:
            sel_val = spec.get(sel)
            if sel_val is not None:
                fltr[sel] = sel_val
        if len(fltr) != len(selector):
            raise Exception(f"For selector={selector} passed spec is missing fields")

        return fltr

    def fetch_one(self, cls, spec, selector=None):
        if obj := self.fetch_one_or_none(cls, spec, selector):
            return obj
        else:
            msg = f"No {cls.__name__} matching {selector} in {spec}"
            raise ValueError(msg)

    def pk_field_from_spec(self, cls, spec):
        pk = cls.get_id_column_name()
        return spec.get(pk)

    @cache
    def fetch_dict_item(self, cls, pk_val):
        pk = cls.get_id_column_name()
        return self.fetch_one(cls, {pk: pk_val})

    def commit(self):
        pass


class TolApiMarshal(TolBaseMarshal):
    def __init__(self):
        self.api_data_source = ApiDataSource(
            {
                "url": os.getenv("TOLQC_URL") + "/tolqc",
                "key": os.getenv("TOLQC_API_KEY"),
            }
        )
        self.model_factory = DefaultDataObjectConverter(
            {m.get_table_name(): m for m in models_list()}
        )

    def make_alchemy_object(self, api_obj):
        return self.model_factory.convert(api_obj)

    def fetch_one_or_none(self, cls, spec, selector=None):
        fltr = self.build_filter(cls, spec, selector)
        obj_type = cls.__tablename__
        ads = self.api_data_source
        stored = list(
            ads.get_list(obj_type, object_filters=DataSourceFilter(exact=fltr))
        )
        count = len(stored)
        if count == 0:
            return None
        elif count == 1:
            return self.make_alchemy_object(stored[0])
        else:
            raise Exception(f"filter {fltr} returned {count} {cls.__name__} objects")

    def fetch_many(self, cls, spec=None, selector=None):
        obj_type = cls.__tablename__
        if spec:
            fltr = self.build_filter(cls, spec, selector)
            return self.api_data_source.get_list(
                obj_type, object_filters=DataSourceFilter(exact=fltr)
            )
        else:
            return self.api_data_source.get_list(obj_type)

    def create(self, cls, spec, selector=None):
        obj_type = cls.__tablename__
        id_ = self.pk_field_from_spec(cls, spec)
        api_obj = ApiObject(obj_type, id_, attributes=spec)
        self.api_data_source.create(api_obj)
        return self.make_alchemy_object(api_obj)

    def fetch_or_create(self, cls, spec, selector=None):
        if api_obj := self.fetch_one_or_none(cls, spec, selector):
            return self.make_alchemy_object(api_obj)
        else:
            return self.create(cls, spec, selector)

    def update_or_create(self, cls, spec, selector=None):
        if api_obj := self.fetch_one_or_none(cls, spec, selector):
            api_attrib = api_obj.attributes
            changed = False
            for prop in self.all_keys_but_primary(cls, api_attrib, spec):
                api_val = api_attrib.get(prop)
                spec_val = spec.get(prop)
                if api_val != spec_val:
                    setattr(api_obj, prop, spec_val)
                    changed = True
            if changed:
                self.api_data_source.update(api_obj)
            return self.make_alchemy_object(api_obj)
        else:
            return self.create(cls, spec, selector)

    def all_keys_but_primary(self, cls, *arg_dicts):
        """Return a list of all the keys in the dict arguments
        which are not the primary key of the class.
        """
        class_pk = cls.get_id_column_name()
        all_props = set()
        for dct in arg_dicts:
            for prop in dct:
                if prop != class_pk:
                    all_props.add(prop)
        return all_props

    def list_projects(self):
        return tuple(self.api_data_source.get_list("project"))


class TolSqlMarshal(TolBaseMarshal):
    def __init__(self, db_alias=None, tz="Europe/London"):
        engine, Session = (
            db_connection.tola_db_engine(db_alias)
            if db_alias
            else db_connection.tola_db_engine()
        )
        self.session = Session()
        self.user_id = self.effective_user_id(self.session)
        self.local_tz = pytz.timezone(tz)

    @staticmethod
    def effective_user_id(ssn):
        user_name = pwd.getpwuid(os.getuid()).pw_name
        ### Difference between filter_by() and where()?
        query = select(User).filter_by(email=f"{user_name}@sanger.ac.uk")
        user = ssn.scalars(query).unique().one()
        return user.id

    def commit(self):
        self.session.commit()

    def fetch_one_or_none(self, cls, spec, selector=None):
        fltr = self.build_filter(cls, spec, selector)
        query = select(cls).filter_by(**fltr)
        return self.session.scalars(query).one_or_none()

    def fetch_many(self, cls, spec=None, selector=None):
        if spec:
            fltr = self.build_filter(cls, spec, selector)
            query = select(cls).filter_by(**fltr)
        else:
            query = select(cls)
        return self.session.scalars(query)

    def create(self, cls, spec, selector=None):
        ssn = self.session
        obj = cls(**spec)
        if issubclass(cls, LogBase):
            self.update_log_fields(obj)
        ssn.add(obj)
        ssn.flush()  # Fetches any auto-generated primary IDs
        return obj

    def fetch_or_create(self, cls, spec, selector=None):
        if obj := self.fetch_one_or_none(cls, spec, selector):
            return obj
        else:
            return self.create(cls, spec, selector)

    def update_or_create(self, cls, spec, selector=None):
        if obj := self.fetch_one_or_none(cls, spec, selector):
            changed = False
            for prop, val in spec.items():
                if type(val) == datetime.datetime:
                    val = self.localize(val, prop)
                if val != getattr(obj, prop):
                    old = getattr(obj, prop)
                    logging.info(
                        f"Changed: {prop} '{'NULL' if old is None else old}' to '{val}'",
                    )
                    setattr(obj, prop, val)
                    changed = True
            if changed and issubclass(cls, LogBase):
                self.update_log_fields(obj)
            return self.session.merge(obj)
        else:
            return self.create(cls, spec, selector)

    @cached_property
    def now(self):
        return datetime.datetime.now(tz=self.local_tz)

    def localize(self, dt, prop):
        if dt.tzinfo and dt.tzinfo.utcoffset(dt) is not None:
            # datetime is "aware"
            return dt
        else:
            # datetime is "naive"
            return self.local_tz.localize(dt)

    def update_log_fields(self, obj):
        obj.modified_at = self.now
        obj.modified_by = self.user_id

    def list_projects(self):
        query = select(Project).where(Project.lims_id is not None)
        # query = select(Project).where(Project.lims_id == 6327)
        return tuple(self.session.scalars(query))

    def add_run_accession(self, acc, file):
        ssn = self.session
        query = (
            select(Data)
            .where(Data.accession_id.is_(None))
            .join(Data.files)
            .where(File.remote_path == file)
        )
        data_rslt = tuple(ssn.scalars(query))
        data_lgth = len(data_rslt)
        if data_lgth == 1:
            data = data_rslt[0]
            data.accession_id = acc
            self.update_log_fields(data)
            ssn.merge(data)
        elif data_lgth > 1:
            msg = (
                f"Expected zero or one Data objects matching"
                f" acc='{acc} 'file='{file}' but found {data_lgth}"
            )
            raise ValueError(msg)
