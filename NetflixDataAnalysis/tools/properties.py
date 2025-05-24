def load_properties(path: str) -> dict:
    props = {}
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key_value = line.split("=", 1)
                if len(key_value) == 2:
                    key, value = key_value
                    props[key.strip()] = value.strip()
    return props
