
def strip_values(x):
    if x:
        return x.strip()
    return ""

def convert_relative_url(url, loader_context):
    return loader_context['response'].urljoin(url)
