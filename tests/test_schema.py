import danio


def test_field_auto_increment_metadata():
    assert danio.IntField(primary=True, auto_increment=True).auto_increment
    assert danio.IntField(primary=True, type="serial").auto_increment
    assert danio.IntField(primary=True, type="bigserial").auto_increment
    assert danio.IntField(primary=True, type="smallserial").auto_increment
    assert not danio.TextField(primary=True).auto_increment
