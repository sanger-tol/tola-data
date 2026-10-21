#!/usr/bin/env python3

import json
import sys

import duckdb


def main():
    conn = duckdb.connect()
    conn.execute("""
      ATTACH 'postgresql://tolqc-dev@127.0.0.1:5435/tolqc'
      AS tqc (TYPE POSTGRES);
    """)

    sql = """
      WITH
        set_n AS (
          SELECT
            asm_set.assembly_id,
            specimen: any_value(asm_set.specimen_id),
            set_name: any_value(asm_set.name),
            version: any_value(split(asm_set.name, '.')[-1]::INT),
            components: list(
              struct_pack(
                assembly_id := cmp.assembly_id,
                component_type := cmp.component_type_id,
                acc := split(cmp.genome_accession_id, '.')[1],
                acc_v := cmp.genome_accession_id,
                level := cmp.level
              )
            ),
            n: count(*)
          FROM
            tqc.assembly AS asm_set
            JOIN tqc.assembly_source ON asm_set.assembly_id = assembly_source.source_assembly_id
            JOIN tqc.assembly AS cmp ON assembly_source.assembly_id = cmp.assembly_id
          WHERE
            asm_set.component_type_id = 'set'
          GROUP BY
            asm_set.assembly_id
        )
      SELECT
        n1_id: n1.assembly_id,
        n1_set: n1.set_name,
        n1_cmpts: n1.components,
        n2_id: n2.assembly_id,
        n2_set: n2.set_name,
        n2_cmpts: n2.components,
      FROM
        set_n AS n1
        JOIN set_n AS n2 ON n1.specimen = n2.specimen
        AND n1.version + 1 = n2.version
      WHERE
        n1.n != n2.n
      ORDER BY n1_set;
    """
    conn.execute(sql)

    # Example components structs:
    #
    #   n1_cmpts = [
    #     {
    #       "assembly_id": 7227,
    #       "component_type": "maternal",
    #       "acc": "GCA_922990625",
    #       "acc_v": "GCA_922990625.1",
    #       "level": "scaffold"
    #     },
    #     {
    #       "assembly_id": 7228,
    #       "component_type": "paternal",
    #       "acc": "GCA_922984935",
    #       "acc_v": "GCA_922984935.1",
    #       "level": "chromosome"
    #     }
    #   ]
    #   n2_cmpts = [
    #     {
    #       "assembly_id": 7229,
    #       "component_type": "haploid",
    #       "acc": "GCA_922984935",
    #       "acc_v": "GCA_922984935.2",
    #       "level": "chromosome"
    #     }
    #   ]
    for n1_id, n1_set, n1_cmpts, n2_id, n2_set, n2_cmpts in conn.fetchall():
        # if not (len(n1_cmpts) == 2 and len(n2_cmpts) == 1):
        #     continue
        if len(n1_cmpts) < len(n2_cmpts):
            continue

        n1 = {x["acc"]: x for x in n1_cmpts}
        n2 = {x["acc"]: x for x in n2_cmpts}

        print(f"\n{n1_set} ({n1_id}) = {json.dumps(n1_cmpts, indent=2)}", file=sys.stderr)
        print(f"{n2_set} ({n2_id}) = {json.dumps(n2_cmpts, indent=2)}", file=sys.stderr)
        for acc, cmp in n1.items():
            if not n2.get(acc):
                print(f'{{"source.id":"{n2_id}","component.id":"{cmp["assembly_id"]}"}}')


if __name__ == "__main__":
    main()
