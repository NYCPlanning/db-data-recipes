def test_get_url():
    from lib.get_url import get_url
    
    url1 = get_url('dcas_colp', 'latest') 
    url2 = get_url('dcas_colp', '2019-04-04')
    assert url2 == 'https://sfo2.digitaloceanspaces.com/sptkl/dcas_colp/2019-04-04/datapackage.json', 'error getting version'
    assert url1 == 'https://sfo2.digitaloceanspaces.com/sptkl/dcas_colp/2019-04-04/datapackage.json', 'error getting latest'