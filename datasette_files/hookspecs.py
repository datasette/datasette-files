from pluggy import HookspecMarker

hookspec = HookspecMarker("datasette")


@hookspec
def register_files_storage_types(datasette):
    "Return a list of Storage subclasses"


@hookspec
def file_actions(datasette, actor, file, preview_bytes):
    "Return a list of {'href': ..., 'label': ..., 'description': ...} dicts for the file actions menu"


@hookspec
def register_thumbnail_generators(datasette):
    "Return a list of ThumbnailGenerator instances"
