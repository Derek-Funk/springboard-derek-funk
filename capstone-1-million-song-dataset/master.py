import importlib

# flip True/False for testing
modules = {
    'download-subset': False,
    'db-non-track': True,
    'db-track': True
}

for module in modules:
    if modules[module]:
        importlib.import_module(module)