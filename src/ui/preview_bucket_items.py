from supervisely.app.widgets import Card, FileViewer

file_viewer = FileViewer(files_list=[])

card = Card(
    title="2️⃣ Preview and select items",
    description="All selected directories will be imported",
    content=file_viewer,
)

card.hide()
