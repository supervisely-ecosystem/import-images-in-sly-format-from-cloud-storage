<div align="center" markdown>
<img src="https://github.com/supervisely-ecosystem/import-images-in-sly-format-from-cloud-storage/assets/119248312/1e1551c4-7aaf-4ad5-b1bf-a850fbd39975"/>

# Import image projects in Supervisely format from cloud storage

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#How-To-Use">How To Use</a>
</p>


[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervise.ly/apps/supervisely-ecosystem/import-images-in-sly-format-from-cloud-storage)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.ly/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/import-images-in-sly-format-from-cloud-storage)
[![views](https://app.supervise.ly/img/badges/views/supervisely-ecosystem/import-images-in-sly-format-from-cloud-storage.png)](https://supervise.ly)
[![runs](https://app.supervise.ly/img/badges/runs/supervisely-ecosystem/import-images-in-sly-format-from-cloud-storage.png)](https://supervise.ly)

</div>

# Overview

This app allows importing image projects from the most popular cloud storage providers to Supervisely private instance.
You can learn about Supervisely format [here](https://docs.supervise.ly/data-organization/00_ann_format_navi).

List of providers:
- Amazon s3
- Google Cloud Storage (CS)
- Microsoft Azure
- and others with s3 compatible interfaces

# How To Use

0. Ask your instance administrator to add cloud credentials to instance settings. It can be done both in .env 
   configuration files or in Admin UI dashboard. Learn more in docs: [link1](https://docs.supervise.ly/enterprise-edition/installation/post-installation#configure-your-instance), 
   [link2](https://docs.supervise.ly/enterprise-edition/advanced-tuning/s3#links-plugin-cloud-providers-support). 
   In case of any questions or issues, please contact tech support.
1. Run app from `Ecosystem` Page.
2. Connect to cloud bucket, preview and select directories with projects, import image projects in Supervisely format to selected Team - Workspace. You can perform these actions as many times as needed.
3. Once you are done with the app, you should close the app manually.

# Screenshot
<div align="center" markdown>
<img src="https://user-images.githubusercontent.com/48913536/297813522-22a81989-9928-45fc-828a-e2bd4ddecee0.png" width=80%/>
</div>
