from supervisely.app.widgets import (
    Button,
    Card,
    Container,
    Progress,
    SelectWorkspace,
    Text,
    Field,
    RadioGroup,
    ReloadableArea,
    Empty,
)

import src.globals as g
import src.ui.connect_to_bucket as connect_to_bucket
import src.ui.preview_bucket_items as preview_bucket_items
import src.ui.utils as utils

duplication_options_items = [
    RadioGroup.Item(
        value="copy", label="Copy file from cloud to Supervisely Storage", content=None
    ),
    RadioGroup.Item(
        value="link",
        label="Keep file only in cloud without duplication, add to Supervisely Storage by link",
        content=None,
    ),
]
duplication_options = RadioGroup(duplication_options_items, direction="vertical")

data_duplication_field = Field(
    title="Data duplication", description="", content=duplication_options
)

destination = SelectWorkspace(default_id=g.WORKSPACE_ID, team_id=g.TEAM_ID)
import_button = Button(text="Start")

progress_bar = Progress()
progress_bar.hide()

progress_bar2 = Progress()
progress_bar2.hide()

output_message = Text()
output_message.hide()


results_widgets = ReloadableArea(Empty())
results_widgets.hide()

destination_container = Container(
    widgets=[
        data_duplication_field,
        destination,
        import_button,
        progress_bar,
        progress_bar2,
        output_message,
        results_widgets,
    ]
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
    progress_bar2.hide()
    output_message.hide()
    results_widgets.hide()
    dst_projects_ids = []
    result_preview_widgets = []

    selected_dirs = [dir["path"] for dir in preview_bucket_items.file_viewer.get_selected_items()]
    provider = connect_to_bucket.provider_selector.get_value()
    bucket_name = connect_to_bucket.bucket_name_selector.get_value()

    dst_ws_id = destination.get_selected_id()
    dst_ws_name = g.api.workspace.get_info_by_id(dst_ws_id).name

    progress_bar.show()
    validated_map = utils.validate_selected_dirs(selected_dirs, provider, bucket_name, progress_bar)
    validated_dirs = list(validated_map.keys())

    if len(validated_map) > 0:
        if duplication_options.get_value() == "copy":
            project_dirs = utils.download_selected_projects(
                validated_dirs, validated_map, progress_bar, progress_bar2
            )
            dst_projects_ids = utils.upload_projects_by_path(
                project_dirs, dst_ws_id, progress_bar, progress_bar2
            )
        else:
            dst_projects_ids = utils.upload_projects_by_links(
                validated_dirs, validated_map, dst_ws_id, progress_bar, progress_bar2
            )

    skipped_projects_count = len(selected_dirs) - len(validated_dirs)
    utils.show_result(
        dst_ws_name,
        dst_ws_id,
        dst_projects_ids,
        output_message,
        result_preview_widgets,
        results_widgets,
        skipped_projects_count,
    )
