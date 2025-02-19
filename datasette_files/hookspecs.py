from pluggy import HookspecMarker

hookspec = HookspecMarker("datasette")


@hookspec
def register_files_storages(datasette):
    "A list of Storage subclass instances for datasette-files"
