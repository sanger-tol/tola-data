from tola.fetch_ont_seq_data import (
    get_irods_ont_last_modified,
    store_irods_ont_last_modified,
    utc_datetime,
)


def test_utc_datetime():
    assert utc_datetime("1985-06-05 12:00").isoformat() == "1985-06-05T11:00:00+00:00"
    assert (
        utc_datetime("1985-06-05 12:00:00-02:00").isoformat()
        == "1985-06-05T14:00:00+00:00"
    )

def test_get_set_last_modified(client):
    stored_val = get_irods_ont_last_modified(client)
    dt = utc_datetime("1985-06-05 16:00:00")
    store_irods_ont_last_modified(client, dt)
    db_dt = get_irods_ont_last_modified(client)
    if stored_val:
        store_irods_ont_last_modified(client, stored_val)
    assert db_dt.isoformat() == "1985-06-05T15:00:00+00:00"
