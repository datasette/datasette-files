from pluggy import HookspecMarker

hookspec = HookspecMarker("datasette")


@hookspec
def register_files_storage_types(datasette):
    "Return a list of Storage subclasses"
