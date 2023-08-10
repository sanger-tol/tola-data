import argparse
import datetime
import logging
import os
import pwd
import pytz
import sys

from functools import cache
from sqlalchemy import select

from main.model import Data, File, Project, User
from tol.api_client import ApiDataSource, ApiObject
from tol.core import DataSourceFilter
from tola import db_connection


def marshal_from_command_line(desc):
    prsr = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    prsr.add_argument(
        "files",
        nargs="*",
        help="List of one or more input files.",
        metavar="FILE_LIST",
    )

    prsr_api_choice = prsr.add_mutually_exclusive_group()
    prsr_api_choice.add_argument(
        "--api",
        action="store_const",
        const=True,
        default=False,
        help="Use ToL API Marshal to store data.",
    )
    prsr_api_choice.add_argument(
        "--sql",
        action="store_const",
        dest="api",
        const=False,
        default=True,
        help="Use ToL SQL Marshal to store data.",
    )

    ns_obj = prsr.parse_args()
    mrshl = TolApiMarshal() if ns_obj.api else TolSqlMarshal()
    return mrshl, ns_obj.files


class TolBaseMarshal:
    def build_filter(self, cls, spec, selector=None):
        # Use the primary key if there is no selector
        if not selector:
            selector = (self.class_pimary_key(cls),)

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

    @cache
    def class_pimary_key(self, cls):
        if hasattr(cls.Meta, 'id_column'):
            return cls.Meta.id_column
        else:
            return 'id'

    def pk_field_from_spec(self, cls, spec):
        pk = self.class_pimary_key(cls)
        return spec.get(pk)

    @cache
    def fetch_dict_item(self, cls, pk_val):
        pk = self.class_pimary_key(cls)
        return self.fetch_one(cls, {pk: pk_val})

    def commit(self):
        pass


class TolApiMarshal(TolBaseMarshal):
    def __init__(self):
        self.api_data_source = ApiDataSource(
            {
                'url': os.getenv('TOLQC_URL') + '/api/v1',
                'key': os.getenv('TOLQC_API_KEY'),
            }
        )

    def make_alchemy_object(self, cls, api_obj):
        spec = api_obj.attributes
        if self.class_pimary_key(cls) == 'id':  ### Not necessary?
            spec["id"] = api_obj.id
        return cls(**spec)

    def fetch_one_or_none(self, cls, spec, selector=None):
        fltr = self.build_filter(cls, spec, selector)
        obj_type = cls.Meta.type_
        ads = self.api_data_source
        stored = list(
            ads.get_list(obj_type, object_filters=DataSourceFilter(exact=fltr))
        )
        count = len(stored)
        if count == 0:
            return None
        elif count == 1:
            return stored[0]
        else:
            raise Exception(f"filter {fltr} returned {count} {cls.__name__} objects")

    def fetch_many(self, cls, spec=None, selector=None):
        obj_type = cls.Meta.type_
        if spec:
            fltr = self.build_filter(cls, spec, selector)
            return self.api_data_source.get_list(
                obj_type, object_filters=DataSourceFilter(exact=fltr)
            )
        else:
            return self.api_data_source.get_list(obj_type)

    def create(self, cls, spec, selector=None):
        obj_type = cls.Meta.type_
        id_ = self.pk_field_from_spec(cls, spec)
        api_obj = ApiObject(obj_type, id_, attributes=spec)
        self.api_data_source.create(api_obj)
        return self.make_alchemy_object(cls, api_obj)

    def fetch_or_create(self, cls, spec, selector=None):
        if api_obj := self.fetch_one_or_none(cls, spec, selector):
            return self.make_alchemy_object(cls, api_obj)
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
            return self.make_alchemy_object(cls, api_obj)
        else:
            return self.create(cls, spec, selector)

    def all_keys_but_primary(self, cls, *arg_dicts):
        """Return a list of all the keys in the dict arguments
        which are not the primary key of the class.
        """
        class_pk = self.class_pimary_key(cls)
        all_props = set()
        for dct in arg_dicts:
            for prop in dct:
                if prop != class_pk:
                    all_props.add(prop)
        return all_props

    def list_projects(self):
        return tuple(self.api_data_source.get_list('projects'))


class TolSqlMarshal(TolBaseMarshal):
    def __init__(self):
        # engine, Session = db_connection.tola_db_engine(echo=True)
        engine, Session = db_connection.tola_db_engine()
        self.session = Session()
        self.user_id = self.effective_user_id(self.session)

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
        if cls.has_log_details():
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
                if val != getattr(obj, prop):
                    old = getattr(obj, prop)
                    logging.info(
                        f"Changed: {prop} '{'NULL' if old is None else old}' to '{val}'",
                        file=sys.stderr,
                    )
                    setattr(obj, prop, val)
                    changed = True
            if changed and cls.has_log_details():
                self.update_log_fields(obj)
            return self.session.merge(obj)
        else:
            return self.create(cls, spec, selector)

    @staticmethod
    def now():
        return datetime.datetime.now(tz=pytz.timezone("Europe/London"))

    def update_log_fields(self, obj):
        now = self.now()
        if not obj.created_at:
            obj.created_at = now
        if not obj.created_by:
            obj.created_by = self.user_id
        obj.last_modified_at = now
        obj.last_modified_by = self.user_id

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
