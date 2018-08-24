import argparse
import requests
import xmltodict


MUNDI_SEARCH_URL = 'https://catalog-browse.default.mundiwebservices.com/acdc/catalog/proxy/search'


def mundi_download_url_for_given_safe(safe_title):
    """
    Use Mundi search API to get the url of a given SAFE.

    Args:
        safe_title (string): title of a safe (without the .SAFE extension)

    Return:
        url to that SAFE zip file hosted at Mundi
    """
    r = requests.get('{}/Sentinel1/opensearch?uid={}'.format(MUNDI_SEARCH_URL, safe_title))
    if r.ok:
        d = xmltodict.parse(r.text)
        return d['feed']['entry']['link'][1]['@href']
    else:
        r.raise_for_status()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of SAFE files on Mundi.')
    parser.add_argument('safename', help=('title of the safe'))
    args = parser.parse_args()
    print(mundi_download_url_for_given_safe(args.safename))
