import argparse
import datetime
import json
import os
import pwd

from main.model import Project, Species, User
from sqlalchemy import select
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

    @staticmethod
    def class_pimary_key(cls):
        if hasattr(cls.Meta, 'id_column'):
            return cls.Meta.id_column
        else:
            return 'id'

    @staticmethod
    def pk_field_from_spec(cls, spec):
        pk = TolBaseMarshal.class_pimary_key(cls)
        return spec.get(pk)

    def commit():
        pass


class TolApiMarshal(TolBaseMarshal):
    def __init__(self):
        self.api_data_source = ApiDataSource(
            {
                'url': os.getenv('TOLQC_URL') + '/api/v1',
                'key': os.getenv('TOLQC_API_KEY'),
            }
        )

    def fetch_or_create(self, cls, spec, selector=None):
        fltr = self.build_filter(cls, spec, selector)
        obj_type = cls.Meta.type_
        ads = self.api_data_source
        stored = list(
            ads.get_list(obj_type, object_filters=DataSourceFilter(exact=fltr))
        )
        count = len(stored)
        if count == 0:
            id_ = self.pk_field_from_spec(cls, spec)
            new_obj = ApiObject(obj_type, id_, attributes=spec)
            print(json.dumps(new_obj.__dict__, indent=2))
            ads.create(new_obj)
            return self.make_alchemy_object(cls, new_obj)
        elif count == 1:
            return self.make_alchemy_object(cls, stored[0])
        else:
            raise Exception(f"filter {fltr} returned {count} {cls.__name__} objects")

    def make_alchemy_object(self, cls, api_obj):
        spec = api_obj.attributes
        if self.class_pimary_key(cls) == 'id':
            spec["id"] = api_obj.id
        return cls(**spec)

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

    def fetch_or_create(self, cls, spec, selector=None):
        fltr = self.build_filter(cls, spec, selector)

        query = select(cls).filter_by(**fltr)
        ssn = self.session
        if db_obj := ssn.scalars(query).one_or_none():
            # Object matching selector fields is already in database
            return db_obj
        else:
            if cls.has_log_details():
                self.add_log_fields(spec)
            obj = cls(**spec)
            ssn.add(obj)
            ssn.flush()
            return obj

    def add_log_fields(self, spec):
        now = datetime.datetime.now(datetime.timezone.utc)
        if not spec.get("created_at"):
            spec["last_modified_at"] = now
        if not spec.get("created_by"):
            spec["created_by"] = self.user_id
        spec["last_modified_at"] = now
        spec["last_modified_by"] = self.user_id

    def list_projects(self):
        query = select(Project).where(Project.lims_id is not None)
        # query = select(Project).where(Project.lims_id == 6327)
        return tuple(self.session.scalars(query))

    def fetch_sci_taxon_dict(self):
        sci_taxon = {}
        for species in self.session.scalars(select(Species)):
            sci_taxon[species.species_id] = species.taxon_id
        return sci_taxon
