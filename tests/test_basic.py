import pytest

ct_list_all = ['taxi_%s.bcolz'%x for x in range(12)]
ct_list_single = ['taxi.bcolz']

@pytest.fixture
def rpc():
    import bqueryd
    return bqueryd.RPC(timeout=15) # Nice short timeout

def test_foo(rpc):
    result = rpc.info()
    assert(type(result) is dict)

def test_plain(rpc):
    assert(rpc.groupby(ct_list_all[0], ['payment_type'], ['nr_rides'], [], aggregate=True) is not None)
    assert(rpc.groupby(ct_list_all[0], ['pickup_yearmonth'], ['nr_rides'], [], aggregate=True) is not None)
    assert(rpc.groupby(ct_list_all[0], ['pickup_yearmonth', 'payment_type'], ['nr_rides'], [], aggregate=True) is not None)

def test_empty_columns(rpc):
    rpc.groupby(ct_list_all[0], [], [], [], aggregate=True)

def test_big(rpc):
    from pandas.util.testing import assert_frame_equal
    parts = rpc.groupby(ct_list_all, ['payment_type'], ['nr_rides'], [])
    big = rpc.groupby(ct_list_single, ['payment_type'], ['nr_rides'], [])
    assert_frame_equal(parts.sum(), big.sum())
