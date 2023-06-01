import os

import supervisely as sly
from supervisely.app.widgets import (
    Button,
    Card,
    Container,
    SelectWorkspace,
    Progress,
    DoneLabel,
)
from supervisely.io.fs import get_file_ext, get_file_name, remove_dir
from supervisely.imaging.image import SUPPORTED_IMG_EXTS
import src.globals as g
import src.ui.connect_to_bucket as connect_to_bucket
import src.ui.preview_bucket_items as preview_bucket_items

destination = SelectWorkspace(default_id=g.WORKSPACE_ID, team_id=g.TEAM_ID)
import_button = Button(text="Start")

progress_bar = Progress()
progress_bar.hide()

output_message = DoneLabel()
output_message.hide()

destination_container = Container(
    widgets=[destination, output_message, import_button, progress_bar]
)

card = Card(
    "3️⃣ Output project",
    "Select output destination",
    collapsable=False,
    content=destination_container,
)

card.hide()


@import_button.click
def import_images_project():
    progress_bar.hide()
    output_message.hide()

    paths = preview_bucket_items.file_viewer.get_selected_items()
    remote_paths = []
    local_paths = []

    provider = connect_to_bucket.provider_selector.get_value()
    bucket_name = connect_to_bucket.bucket_name_input.get_value()

    def _add_to_processing_list(path):
        nonlocal remote_paths, local_paths

        full_remote_path = f"{provider}://{path.lstrip('/')}"
        remote_paths.append(full_remote_path)
        local_path = os.path.join(g.STORAGE_DIR, path.lstrip("/"))
        sly.fs.ensure_base_path(local_path)
        local_paths.append(local_path)

    # find selected directories
    project_names = []
    projects_map = {}
    selected_dirs = []
    for path in paths:
        if sly.fs.get_file_ext(path) == "":
            # path to directory
            selected_dirs.append(path)

    # get all files from selected dirs
    if len(selected_dirs) > 0:
        g.FILE_SIZE = {}
        for dir_path in selected_dirs:
            full_dir_path = f"{provider}://{dir_path.strip('/')}"
            files_cnt = 0
            for file in list_objects(g.api, full_dir_path):
                if file["size"] <= 0:
                    continue

                path = os.path.join(f"/{bucket_name}", file["prefix"], file["name"])
                g.FILE_SIZE[path] = file["size"]
                files_cnt += 1
                if files_cnt % 10000 == 0:
                    sly.logger.info(f"Listing files from remote storage {files_cnt}")

        for path in g.FILE_SIZE.keys():
            if path in selected_dirs:
                continue
            if path.startswith(tuple(selected_dirs)):
                _add_to_processing_list(path)

    # get other selected files
    for path in paths:
        if sly.fs.get_file_ext(path) != "":
            _add_to_processing_list(path)

    if len(local_paths) == 0:
        raise sly.app.DialogWindowWarning(
            title="There are no images to import",
            description="Nothing to download",
        )

    dst_ws_id = destination.get_selected_id()
    dst_ws_name = g.api.workspace.get_info_by_id(dst_ws_id).name
    progress_bar.show()
    with progress_bar(message="Importing items", total=len(local_paths) * 2) as pbar:
        for batch_remote_paths, batch_local_paths in zip(
            sly.batched(remote_paths, batch_size=g.BATCH_SIZE),
            sly.batched(local_paths, batch_size=g.BATCH_SIZE),
        ):
            for remote_path, local_path in zip(batch_remote_paths, batch_local_paths):
                g.api.remote_storage.download_path(remote_path, local_path)
                pbar.update()

        for dir in selected_dirs:
            project_name = os.path.basename(dir)
            project_path = os.path.join(g.STORAGE_DIR, dir.lstrip("/"))
            res_proj_id, res_proj_name = sly.upload_project(
                dir=project_path,
                api=g.api,
                workspace_id=dst_ws_id,
                project_name=project_name,
                progress_cb=progress_bar,
            )
            sly.logger.info(f"Project '{res_proj_name}' has been uploaded")

    output_message.text = f"{len(selected_dirs)} projects have been imported to workspace: {dst_ws_name} ID: {dst_ws_id}"
    output_message.show()


def list_objects(api, full_dir_path):
    start_after = None
    last_obj = None
    while True:
        remote_objs = api.remote_storage.list(
            path=full_dir_path,
            files=True,
            folders=False,
            recursive=True,
            start_after=start_after,
        )
        if len(remote_objs) == 0:
            break
        if last_obj is not None:
            if remote_objs[-1] == last_obj:
                break
        last_obj = remote_objs[-1]
        start_after = f'{last_obj["prefix"]}/{last_obj["name"]}'
        yield from remote_objs
