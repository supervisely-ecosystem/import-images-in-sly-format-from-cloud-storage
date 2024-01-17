import os

import supervisely as sly
from supervisely.app.widgets import Button, Card, Container, Progress, SelectWorkspace, Text, Field, Checkbox, RadioGroup, ProjectThumbnail, Flexbox, ReloadableArea, Empty

import src.globals as g
import src.ui.connect_to_bucket as connect_to_bucket
import src.ui.preview_bucket_items as preview_bucket_items
from supervisely.io.json import load_json_file
from supervisely.io.fs import silent_remove, get_file_name_with_ext, remove_dir
from supervisely import ProjectInfo

duplication_options_items = [
    RadioGroup.Item(value="copy", label="Copy file from cloud to Supervisely Storage", content=None),
    RadioGroup.Item(value="link", label="Keep file only in cloud without duplication, add to Supervisely Storage by link", content=None)
]
duplication_options = RadioGroup(duplication_options_items, direction="vertical")

data_duplication_field = Field(
    title="Data duplication",
    description="",
    content=duplication_options
)

destination = SelectWorkspace(default_id=g.WORKSPACE_ID, team_id=g.TEAM_ID)
import_button = Button(text="Start")

progress_bar = Progress()
progress_bar.hide()

output_message = Text()
output_message.hide()

# info_message = Text()
# info_message.hide()

results_widgets = ReloadableArea(Empty())
results_widgets.hide()

destination_container = Container(
    widgets=[data_duplication_field, destination, import_button, progress_bar, output_message, results_widgets]
)


card = Card(
    "3️⃣ Output project",
    "Configure data copying and destination project",
    collapsable=False,
    content=destination_container,
)

card.hide()


@import_button.click
def import_images_project():
    progress_bar.hide()
    output_message.hide()
    results_widgets.hide()
    # info_message.hide()
    result_projects_ids = []
    result_preview_widgets = []

    paths = preview_bucket_items.file_viewer.get_selected_items()
    remote_paths = []
    local_paths = []

    provider = connect_to_bucket.provider_selector.get_value()
    bucket_name = connect_to_bucket.bucket_name_selector.get_value()

    def _add_to_processing_list(path):
        nonlocal remote_paths, local_paths

        full_remote_path = f"{provider}://{path.lstrip('/')}"
        remote_paths.append(full_remote_path)
        local_path = os.path.join(g.STORAGE_DIR, path.lstrip("/"))
        sly.fs.ensure_base_path(local_path)
        local_paths.append(local_path)

    # find selected directories
    selected_dirs = []
    for path in paths:
        if path["type"] == "folder":
            # path to directory
            selected_dirs.append(path["path"])

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
        if path["type"] == "file":
            _add_to_processing_list(path["path"])

    if len(local_paths) == 0:
        raise sly.app.DialogWindowWarning(
            title="There are no projects to import",
            description="Nothing to download",
        )

    dst_ws_id = destination.get_selected_id()
    dst_ws_name = g.api.workspace.get_info_by_id(dst_ws_id).name
    progress_bar.show()
    
    if duplication_options.get_value() == "copy":
        g.BATCH_SIZE = 50
    
        with progress_bar(
            message="Getting projects from cloud storage", total=len(local_paths)
        ) as pbar:
            for batch_remote_paths, batch_local_paths in zip(
                sly.batched(remote_paths, batch_size=g.BATCH_SIZE),
                sly.batched(local_paths, batch_size=g.BATCH_SIZE),
            ):
                for remote_path, local_path in zip(batch_remote_paths, batch_local_paths):
                    g.api.remote_storage.download_path(remote_path, local_path)
                    pbar.update()

    with progress_bar(
        message="Uploading projects to Supervisely", total=len(selected_dirs)
    ) as pbar:
        for dir in selected_dirs:
            if duplication_options.get_value() == "copy":
                project_name = os.path.basename(dir)
                project_path = os.path.join(g.STORAGE_DIR, dir.lstrip("/"))
                res_proj_id, res_proj_name = sly.upload_project(
                    dir=project_path, api=g.api, workspace_id=dst_ws_id, project_name=project_name
                )
                sly.logger.info(f"Project: '{res_proj_name}' (ID: '{res_proj_id}') has been uploaded")
                pbar.update()
                if res_proj_id is not None:
                    result_projects_ids.append(res_proj_id)
            else:
                dst_project = upload_project_by_links(dir, provider, bucket_name, dst_ws_id)
                if dst_project is not None:
                    result_projects_ids.append(dst_project.id)       
                
    if len(result_projects_ids) == 0:
        output_message.set(
            (
                "No projects have been imported. "
                "Please select directory with project in Supervisely format. Check logs for more information."
            ),
            status="error",
        )


        
    if len(result_projects_ids) > 0:
        output_project_text = "project" if len(selected_dirs) == 1 else "projects"
        output_message.set(
            text=(
                f"{len(selected_dirs)} {output_project_text} have "
                f"been imported to workspace: '{dst_ws_name}' ID: '{dst_ws_id}'"
            ),
            status="success",
        )
        
        result_preview_widgets.append(
            Flexbox(
                widgets=[
                    Text("Projects: "),
                    *[
                        ProjectThumbnail(g.api.project.get_info_by_id(project_id))
                        for project_id in result_projects_ids
                    ],
                ]
            )
        )
        results_widgets.set_content(Container(result_preview_widgets))
        results_widgets.reload()
        results_widgets.show()
        

    # info_message.set(text="You can continue importing projects to another Teams/Workspace. Please, finish the app manually when done", status="info")
    output_message.show()
    # info_message.show()


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

def upload_project_by_links(dir: str, provider: str, bucket_name: str, dst_ws_id: int) -> ProjectInfo:
    project_name = os.path.basename(dir)
    dst_project = g.api.project.create(dst_ws_id, project_name, change_name_if_conflict=True)
    remote_project_dir = f"{provider}://{dir.lstrip('/')}"
    project_files = g.api.remote_storage.list(remote_project_dir, False)
    if len(project_files) == 0:
        sly.logger.warn(f"Project '{remote_project_dir}' is empty. Skipping...")
        return
    
    remote_meta_path = [f for f in project_files if f["name"] == "meta.json" and f["type"] == "file"]
    if len(remote_meta_path) == 0:
        sly.logger.warn(f"'meta.json' file not found in {dir}. Skipping...")
        return
    
    remote_meta_path = f"{provider}://{bucket_name}/{remote_meta_path[0]['prefix']}/{remote_meta_path[0]['name']}"
    try:
        local_meta_path = os.path.join(g.STORAGE_DIR, dir.lstrip("/"), "meta.json")
        g.api.remote_storage.download_path(remote_meta_path, local_meta_path)
        meta_json = load_json_file(local_meta_path)
        meta = sly.ProjectMeta.from_json(meta_json)
        g.api.project.update_meta(dst_project.id, meta)
        silent_remove(local_meta_path)
    except:
        sly.logger.warn(f"Couldn't download 'meta.json' file from '{remote_meta_path}'. Skipping...")
        return
    
    datasets = [f for f in project_files if f["type"] == "folder"]
    for dataset in datasets:
        dataset_name = dataset["name"]
        remote_dataset_path = f"{provider}://{bucket_name}/{dataset['prefix']}/{dataset['name']}"
        dataset_files = g.api.remote_storage.list(remote_dataset_path, True)
        image_links = []
        image_names = []
        annotation_links = []
        for file in dataset_files:
            remote_file_path = f"{provider}://{bucket_name}/{file['prefix']}/{file['name']}"
            base_dir = os.path.basename(os.path.basename(file["prefix"]))
            if base_dir == "img":
                image_names.append(file["name"])
                image_links.append(remote_file_path)
            elif base_dir == "ann":
                annotation_links.append(remote_file_path)
            else:
                continue
            
        if len(image_links) == 0:
            sly.logger.warn(f"No images found in dataset: '{remote_dataset_path}'. Skipping...")
            continue
        
        dst_dataset = g.api.dataset.create(dst_project.id, dataset_name, change_name_if_conflict=True)
        dst_images = g.api.image.upload_links(dst_dataset.id, image_names, image_links) # add progress
        dst_images_ids = [image_info.id for image_info in dst_images]
        ann_jsons = []
        for remote_ann_path in annotation_links:
            local_ann_dir = os.path.join(g.STORAGE_DIR, dir.lstrip("/"), dataset_name, "ann")
            local_ann_path = os.path.join(local_ann_dir, get_file_name_with_ext(remote_ann_path))
            g.api.remote_storage.download_path(remote_ann_path, local_ann_path)
            ann_jsons.append(load_json_file(local_ann_path))
            
        remove_dir(local_ann_dir)
        g.api.annotation.upload_jsons(dst_images_ids, ann_jsons)
    return dst_project