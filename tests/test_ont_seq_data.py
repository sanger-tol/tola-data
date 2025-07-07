from tola.fetch_ont_seq_data import utc_datetime


def test_utc_datetime():
    assert (
        utc_datetime("1985-06-05 12:00").isoformat() == "1985-06-05T12:00:00+00:00"
    )
    assert (
        utc_datetime("1985-06-05 12:00:00-02:00").isoformat() == "1985-06-05T14:00:00+00:00"
    )
