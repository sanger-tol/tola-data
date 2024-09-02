#!/usr/bin/env python3

import sys

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Assembly(Base):
    __tablename__ = "assembly"

    assembly_id = Column(Integer, primary_key=True)
    software_version_id = Column(Integer)
    dataset_id = Column(Integer)
    component_type_id = Column(String)
    assembly_status_id = Column(Integer)
    name = Column(String)
    description = Column(String)

    # Sources are assemblies for which there is a row in assembly_source
    # with this instance's assembly_id
    source_assembly_assn = relationship(
        "AssemblySource",
        primaryjoin="Assembly.assembly_id == AssemblySource.assembly_id",
        back_populates="component",
    )
    sources = association_proxy("source_assembly_assn", "source")

    # Components are assemblies which have this assembly as their source
    component_assembly_assn = relationship(
        "AssemblySource",
        primaryjoin="Assembly.assembly_id == AssemblySource.source_assembly_id",
        back_populates="source",
    )
    components = association_proxy("component_assembly_assn", "component")


class AssemblySource(Base):
    __tablename__ = "assembly_source"

    assembly_id = Column(Integer, ForeignKey("assembly.assembly_id"), primary_key=True)
    source_assembly_id = Column(
        Integer, ForeignKey("assembly.assembly_id"), primary_key=True
    )

    source = relationship(
        "Assembly",
        foreign_keys=[source_assembly_id],
        back_populates="component_assembly_assn",
    )
    component = relationship(
        "Assembly",
        foreign_keys=[assembly_id],
        back_populates="source_assembly_assn",
    )


def main():
    # inheritance(Assembly)
    test_creation_of_all_classes()
    engine = create_engine("sqlite:///assembly.sqlite", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    ssn = Session()
    assembly_factory(ssn)
    assembly_source_factory(ssn)
    ssn.commit()

    # Fetch assembly #11 and print its components
    asm_11 = ssn.query(Assembly).filter_by(assembly_id=11).first()
    print(asm_11.name, asm_11.description)
    for cmp in asm_11.components:
        print("Component: ", cmp.name, cmp.component_type_id, cmp.description)

    # Fetch assembly #15 and print its sources
    asm_15 = ssn.query(Assembly).filter_by(assembly_id=15).first()
    print(asm_15.name, asm_15.description)
    for src in asm_15.sources:
        print("Source: ", src.name, src.component_type_id, src.description)


def assembly_factory(ssn):
    fields = "assembly_id", "name", "component_type_id", "description"
    data = [
        #
        # hifiasm assembly produces three components
        (11, "hifiasm", None, "Initial assembly"),
        (12, "hifiasm", "primary", "Primary haplotype component assembly"),
        (13, "hifiasm", "alternate", "Alternate haplotype component assembly"),
        (14, "hifiasm", "mitochondrion", "Mitochondrion component assembly"),
        #
        # purge_dups assembly
        (15, "purge_dups", None, "purge_dups assembly from hifiasm"),
        (16, "purge_dups", "primary", "purge_dups assembly from hifiasm"),
        (17, "purge_dups", "alternate", "purge_dups assembly from hifiasm"),
        #
        # scaffolding assembly
        (18, "yahs", "primary", "yahs scaffolding assembly from purge_dups"),
        (19, "yahs", "alternate", "yahs scaffolding assembly from purge_dups"),
    ]
    build_and_merge(ssn, Assembly, fields, data)


def assembly_source_factory(ssn):
    fields = "assembly_id", "source_assembly_id"
    data = [
        #
        # Connect the three components of the hifiasm assembly
        (12, 11),
        (13, 11),
        (14, 11),
        #
        # Connect the two sources of the purge_dups assembly
        (15, 12),
        (15, 13),
        #
        # Connect the two components of the purge_dups assembly
        (16, 15),
        (17, 15),
        #
        # Connect each of the yahs scaffolding assembly to its
        # purge_dups component source
        (18, 16),
        (19, 17),
    ]
    build_and_merge(ssn, AssemblySource, fields, data)


def build_and_merge(ssn, Class, fields, data):
    for row in data:
        obj = Class()
        for fld, val in zip(fields, row, strict=True):
            setattr(obj, fld, val)
        ssn.merge(obj)


def inheritance(obj):
    for cls in obj.__mro__:
        print(f"{cls.__name__}: {cls.__module__}", file=sys.stderr)


def test_creation_of_all_classes():
    for mapr in Base.registry.mappers:
        mapr_class = mapr.class_
        mapr_class()


if __name__ == "__main__":
    main()
