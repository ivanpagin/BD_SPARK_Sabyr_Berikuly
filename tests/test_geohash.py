from src.geohash import geohash_encode

def test_geohash_len4():
    g = geohash_encode(18.6251, -111.09, 4)
    assert g is not None
    assert len(g) == 4
