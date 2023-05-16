#!/usr/bin/env python3

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
    # with this assembly's ID
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
    # print(f"{Assembly.__table__!r}")
    engine = create_engine("sqlite:///assembly.sqlite", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    ssn = Session()
    assembly_factory(ssn)
    assembly_source_factory(ssn)
    ssn.commit()

    asm_1 = ssn.query(Assembly).filter_by(assembly_id=1).first()
    print(asm_1.name, asm_1.description)
    for cmp in asm_1.components:
        print("Component: ", cmp.name, cmp.component_type_id, cmp.description)

    asm_5 = ssn.query(Assembly).filter_by(assembly_id=5).first()
    print(asm_5.name, asm_5.description)
    for src in asm_5.sources:
        print("Source: ", src.name, src.component_type_id, src.description)


def assembly_factory(ssn):
    fields = "assembly_id", "name", "component_type_id", "description"
    data = [
        # hifiasm assembly produces three components
        (1, "hifiasm", None, "Initial assembly"),
        (2, "hifiasm", "primary", "Primary haplotype component assembly"),
        (3, "hifiasm", "alternate", "Alternate haplotype component assembly"),
        (4, "hifiasm", "mitochondrion", "Mitochondrion component assembly"),
        # purge_dups assembly
        (5, "purge_dups", None, "purge_dups assembly from hifiasm"),
        (6, "purge_dups", "primary", "purge_dups assembly from hifiasm"),
        (7, "purge_dups", "alternate", "purge_dups assembly from hifiasm"),
        # scaffolding assembly
        (8, "yahs", "primary", "yahs scaffolding assembly from purge_dups"),
        (9, "yahs", "alternate", "yahs scaffolding assembly from purge_dups"),
    ]
    build_and_merge(ssn, Assembly, fields, data)


def assembly_source_factory(ssn):
    fields = "assembly_id", "source_assembly_id"
    data = [
        # Connect the three components of the hifiasm assembly
        (2, 1),
        (3, 1),
        (4, 1),
        # Connect the two sources of the purge_dups assembly
        (5, 2),
        (5, 3),
        # Connect the two components of the purge_dups assembly
        (6, 5),
        (7, 5),
        # Connect each of the yahs scaffolding assembly to its
        # purge_dups component source
        (8, 6),
        (9, 7),
    ]
    build_and_merge(ssn, AssemblySource, fields, data)


def build_and_merge(ssn, Class, fields, data):
    for row in data:
        obj = Class()
        for fld, val in zip(fields, row):
            setattr(obj, fld, val)
        ssn.merge(obj)


if __name__ == "__main__":
    main()
