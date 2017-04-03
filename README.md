# bricker - a Databricks CLI tool

bricker is a utility to sync the local filesystem with DBC. Can only do one way at a time, either up or down. It deletes any notebooks in the target not present in the source, and copies all source notebooks to the target, overwriting any existing notebooks.

# Prerequisites

You need to have git and python installed on your local machine.

# Setup

In the command line (cmd.exe on Windows), run the following command to install bricker (or to update to the lastest version).
```
pip install --upgrade bricker
```

# Usage

First, you need to add a bricker.yml file to your repo to tell bricker how you want the Databricks integration to work. You can use the bricker.template.yml as a starting point.

You then need to add a `_netrc` file to your home path with your login credentials for the Databricks server (see https://www.labkey.org/home/Documentation/wiki-page.view?name=netrc for instructions)

Then, to run, simply go to your datahub repo folder, make sure you've checked out the branch you want to sync, and run:
```
bricker [up/down]
```

The DBC folder is chosen based on the current local git branch:

Local git branch | Remote folder
-----------------|--------------
prod             | /datahub_prod/
Anything else    | /datahub_branches/[local branch name]

NB: a confirmation is need to push to prod, to avoid unplanned madness.

# Moar stuff

We've also added a configurable create cluster method. We found that we're always spinning up dev clusters, and we have quite a bit of configuration that needs to be set up the same way every time. So, if you add your cluster settings to your bricker.yml you can run
```
bricker create_cluster [--num_workers=X] [--cluster_name=Y]
```
and have bricker setup your cluster just like you want it.
