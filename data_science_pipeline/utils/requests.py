import urllib3

import requests



def requests_retry_session(
        retries=10,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 504),
        method_whitelist=('GET', 'HEAD', 'OPTIONS'),
        session=None,
        **kwargs):
    session = session or requests.Session()
    retry = urllib3.util.retry.Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=method_whitelist,
        **kwargs
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
