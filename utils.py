from urllib.parse import urlsplit

def infer_parse_adlpath(path):
    if re.match(r'^[a-zA-Z]:[\\/]', urlpath):
        return {'protocol': 'file',
                'path': urlpath}

    parsed_path = urlsplit(urlpath)
    
    path = path.split('/', 1)[1]
    