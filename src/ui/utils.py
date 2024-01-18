import os
from typing import List
import supervisely as sly
from supervisely import ProjectInfo, batched, Project

import src.globals as g

from supervisely.io.json import load_json_file, dump_json_file
from supervisely.io.fs import silent_remove, remove_dir, mkdir
from supervisely.app.widgets import (
    Container,
    Flexbox,
    Text,
    ProjectThumbnail,
    ReloadableArea,
    Progress,
)


def validate_selected_dirs(
    selected_dirs: List[str], provider: str, bucket_name: str, progress_bar: Progress
) -> dict:
    """
    Returns dict with validated dirs in format:
        {
            dir: {
                "project_name": project_name,
                "project_meta": meta,
                "datasets": [
                    {
                        "dataset_name": dataset_name
                        "images": {"names": names, "links": links}
                        "annotations": {"names": names, "links": links}
                    }
                ]
            }
        }
    """
    validated_map = {dir: {} for dir in selected_dirs}
    with progress_bar(message="Validating selected directories", total=len(selected_dirs)) as pbar:
        for dir in selected_dirs:
            project_name = os.path.basename(dir)
            validated_map[dir]["project_name"] = project_name

            remote_project_dir = f"{provider}://{dir.lstrip('/')}"
            project_files = g.api.remote_storage.list(remote_project_dir, False)
            if len(project_files) == 0:
                sly.logger.warn(f"Project directory'{remote_project_dir}' is empty. Skipping...")
                validated_map.pop(dir)
                pbar.update()
                continue
            remote_meta_path = [
                f for f in project_files if f["name"] == "meta.json" and f["type"] == "file"
            ]
            if len(remote_meta_path) == 0:
                sly.logger.warn(f"'meta.json' file not found in {remote_project_dir}. Skipping...")
                validated_map.pop(dir)
                pbar.update()
                continue

            remote_meta_path = f"{provider}://{bucket_name}/{remote_meta_path[0]['prefix']}/{remote_meta_path[0]['name']}"
            try:
                local_meta_path = os.path.join(g.STORAGE_DIR, dir.lstrip("/"), "meta.json")
                g.api.remote_storage.download_path(remote_meta_path, local_meta_path)
            except:
                sly.logger.warn(
                    f"Couldn't download 'meta.json' file from '{remote_meta_path}'. Skipping..."
                )
                validated_map.pop(dir)
                pbar.update()
                continue

            try:
                meta_json = load_json_file(local_meta_path)
                meta = sly.ProjectMeta.from_json(meta_json)
                validated_map[dir]["project_meta"] = meta
                silent_remove(local_meta_path)
            except:
                sly.logger.warn(
                    (
                        f"There's something wrong with 'meta.json' file from '{remote_meta_path}'. "
                        "Please, check if it's in valid project meta format. Skipping..."
                    )
                )
                validated_map.pop(dir)
                pbar.update()
                continue

            datasets = [f for f in project_files if f["type"] == "folder"]
            if len(datasets) == 0:
                sly.logger.warn(
                    f"No datasets found in project: '{remote_project_dir}'. Skipping..."
                )
                validated_map.pop(dir)
                pbar.update()
                continue

            validated_map[dir]["datasets"] = []
            for dataset in datasets:
                dataset_name = dataset["name"]
                remote_dataset_path = (
                    f"{provider}://{bucket_name}/{dataset['prefix']}/{dataset['name']}"
                )
                dataset_folders = [
                    file
                    for file in g.api.remote_storage.list(remote_dataset_path, False, False, True)
                    if file["name"] in ["img", "ann"]
                ]
                if len(dataset_folders) != 2:
                    sly.logger.warn(
                        (
                            f"Dataset '{remote_dataset_path}' is not valid. "
                            "Dataset dir must contain folders 'img' and 'ann'. Skipping..."
                        )
                    )
                    continue

                image_links = []
                image_names = []
                annotation_names = []
                annotation_links = []
                for ds_folder in dataset_folders:
                    base_dir = ds_folder["name"]
                    remote_base_dir_path = (
                        f"{provider}://{bucket_name}/{ds_folder['prefix']}/{ds_folder['name']}"
                    )
                    if base_dir == "img":
                        image_files = g.api.remote_storage.list(
                            remote_base_dir_path, False, True, False
                        )
                        if len(image_files) == 0:
                            sly.logger.warn(
                                f"No images found in dataset: '{remote_dataset_path}'. Skipping..."
                            )
                            break
                        image_names.extend([file["name"] for file in image_files])
                        image_links.extend(
                            [
                                f"{provider}://{bucket_name}/{file['prefix']}/{file['name']}"
                                for file in image_files
                            ]
                        )
                    if base_dir == "ann":
                        annotation_files = g.api.remote_storage.list(
                            remote_base_dir_path, False, True, False
                        )
                        if len(annotation_files) == 0:
                            sly.logger.warn(
                                f"No annotations found in dataset: '{remote_dataset_path}'. Skipping..."
                            )
                            continue
                        annotation_names.extend([file["name"] for file in annotation_files])
                        annotation_links.extend(
                            [
                                f"{provider}://{bucket_name}/{file['prefix']}/{file['name']}"
                                for file in annotation_files
                            ]
                        )
                if len(image_names) != len(annotation_names):
                    sly.logger.warn(
                        (
                            f"Number of images and annotations in dataset '{remote_dataset_path}' "
                            "is not equal. Skipping..."
                        )
                    )
                    continue

                validated_map[dir]["datasets"].append(
                    {
                        "dataset_name": dataset_name,
                        "images": {"names": image_names, "links": image_links},
                        "annotations": {"names": annotation_names, "links": annotation_links},
                    },
                )
            if len(validated_map[dir]["datasets"]) == 0:
                validated_map.pop(dir)
            pbar.update()
        return validated_map


def download_selected_projects(
    selected_dirs: str, validated_map: dict, progress_bar: Progress, progress_bar2: Progress
) -> List[str]:
    with progress_bar(
        message="Downloading projects from cloud storage", total=len(selected_dirs)
    ) as pbar:
        project_dirs = []
        for dir in selected_dirs:
            project_map = validated_map[dir]
            project_name = project_map["project_name"]
            project_path = os.path.join(g.STORAGE_DIR, project_name)
            mkdir(project_path, True)

            project_meta = project_map["project_meta"]
            project_meta_json = project_meta.to_json()
            dump_json_file(project_meta_json, os.path.join(project_path, "meta.json"))

            dataset_maps = project_map["datasets"]
            for dataset_map in dataset_maps:
                dataset_name = dataset_map["dataset_name"]
                dataset_path = os.path.join(project_path, dataset_name)
                mkdir(dataset_path, True)

                dataset_img_path = os.path.join(dataset_path, "img")
                mkdir(dataset_img_path, True)
                dataset_ann_path = os.path.join(dataset_path, "ann")
                mkdir(dataset_ann_path, True)

                dataset_images = dataset_map["images"]
                dataset_annotations = dataset_map["annotations"]

                with progress_bar2(
                    message=f"Downloading images for dataset: '{dataset_name}'",
                    total=len(dataset_images["names"]),
                ) as pbar2:
                    progress_bar2.show()
                    for image_name, image_link in zip(
                        dataset_images["names"], dataset_images["links"]
                    ):
                        local_img_path = os.path.join(dataset_img_path, image_name)
                        g.api.remote_storage.download_path(image_link, local_img_path)
                        pbar2.update()
                    progress_bar2.hide()

                with progress_bar2(
                    message=f"Downloading annotations for dataset: '{dataset_name}'",
                    total=len(dataset_annotations["names"]),
                ) as pbar2:
                    progress_bar2.show()
                    for ann_name, ann_link in zip(
                        dataset_annotations["names"], dataset_annotations["links"]
                    ):
                        local_ann_path = os.path.join(dataset_ann_path, ann_name)
                        g.api.remote_storage.download_path(ann_link, local_ann_path)
                        pbar2.update()
                    progress_bar2.hide()

            project_dirs.append(project_path)
            pbar.update()
        return project_dirs


def upload_projects_by_path(
    project_dirs: List[str], dst_ws_id: int, progress_bar: Progress, progress_bar2: Progress
) -> List[int]:
    dst_projects_ids = []
    with progress_bar(message="Uploading projects to Supervisely", total=len(project_dirs)) as pbar:
        for project_dir in project_dirs:
            project_name = os.path.basename(project_dir)
            project = Project(project_dir, sly.OpenMode.READ)
            with progress_bar2(
                message=f"Uploading: '{project_name}'", total=project.total_items
            ) as pbar2:
                progress_bar2.show()
                res_proj_id, res_proj_name = sly.upload_project(
                    dir=project_dir,
                    api=g.api,
                    workspace_id=dst_ws_id,
                    project_name=project_name,
                    progress_cb=pbar2.update,
                )
                progress_bar2.hide()
        if res_proj_id is not None:
            dst_projects_ids.append(res_proj_id)
            sly.logger.info(f"Project: '{res_proj_name}' (ID: '{res_proj_id}') has been uploaded")
        pbar.update()
    return dst_projects_ids


def upload_projects_by_links(
    selected_dirs: str,
    validated_map: dict,
    dst_ws_id: int,
    progress_bar: Progress,
    progress_bar2: Progress,
) -> List[ProjectInfo]:
    dst_projects_ids = []
    with progress_bar(
        message="Uploading projects to Supervisely", total=len(selected_dirs)
    ) as pbar:
        for dir in selected_dirs:
            project_map = validated_map[dir]
            project_name = project_map["project_name"]
            project_meta = project_map["project_meta"]
            dataset_maps = project_map["datasets"]
            dst_project = g.api.project.create(
                dst_ws_id, project_name, change_name_if_conflict=True
            )
            g.api.project.update_meta(dst_project.id, project_meta)
            for dataset_map in dataset_maps:
                dataset_name = dataset_map["dataset_name"]
                dataset_images = dataset_map["images"]
                dataset_annotations = dataset_map["annotations"]
                dst_dataset = g.api.dataset.create(
                    dst_project.id, dataset_name, change_name_if_conflict=True
                )

                with progress_bar2(
                    message=f"Uploading images to dataset: '{dataset_name}'",
                    total=len(dataset_images["names"]),
                ) as pbar2:
                    progress_bar2.show()
                    dst_images_ids = []
                    for batch_images_names, batch_images_links in zip(
                        batched(dataset_images["names"]), batched(dataset_images["links"])
                    ):
                        dst_images = g.api.image.upload_links(
                            dst_dataset.id, batch_images_names, batch_images_links
                        )
                        dst_images_ids.extend([image_info.id for image_info in dst_images])
                        pbar2.update(len(batch_images_names))
                    progress_bar2.hide()

                local_ann_dir = os.path.join(g.STORAGE_DIR, dir.lstrip("/"), dataset_name, "ann")

                with progress_bar2(
                    message=f"Uploading annotations to dataset: '{dataset_name}'",
                    total=len(dataset_annotations["names"]),
                ) as pbar2:
                    progress_bar2.show()
                    for batch_images_ids, batch_ann_names, batch_ann_links in zip(
                        batched(dst_images_ids),
                        batched(dataset_annotations["names"]),
                        batched(dataset_annotations["links"]),
                    ):
                        ann_jsons = []
                        for ann_name, ann_link in zip(batch_ann_names, batch_ann_links):
                            local_ann_path = os.path.join(local_ann_dir, ann_name)
                            g.api.remote_storage.download_path(ann_link, local_ann_path)
                            ann_jsons.append(load_json_file(local_ann_path))
                        g.api.annotation.upload_jsons(batch_images_ids, ann_jsons)
                        pbar2.update(len(batch_ann_names))
                    remove_dir(local_ann_dir)
                    progress_bar2.hide()

            sly.logger.info(
                f"Project: '{dst_project.name}' (ID: '{dst_project.id}') has been uploaded"
            )
            dst_projects_ids.append(dst_project.id)
            pbar.update()
        return dst_projects_ids


def list_objects(full_dir_path: str):
    start_after = None
    last_obj = None
    while True:
        remote_objs = g.api.remote_storage.list(
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


def show_result(
    dst_ws_name: str,
    dst_ws_id: int,
    result_projects_ids: List[int],
    output_message: Text,
    result_preview_widgets: List[Flexbox],
    results_widgets: ReloadableArea,
) -> None:
    if len(result_projects_ids) == 0:
        output_message.set(
            (
                "No projects have been imported. "
                "Please select directory with project in Supervisely format. "
                "Check logs for more information."
            ),
            status="error",
        )

    if len(result_projects_ids) > 0:
        output_project_text = "project" if len(result_projects_ids) == 1 else "projects"
        output_message.set(
            text=(
                f"{len(result_projects_ids)} {output_project_text} have "
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
    output_message.show()
