#!/usr/bin/env python
import requests, base64, logging, os, errno, shutil, sys, yaml
from pprint import pprint
from urlparse import urlparse
from multiprocessing import Pool
import click
import git
from easydict import EasyDict

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout,level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)

def local_repo(): return git.Repo('.')
def local_repo_active_branch(): return local_repo().active_branch.name

def settings():
    return yaml.load(file('bricker.yml', 'r'))

def dbc_base():
    if local_repo_active_branch() == settings()['github_branches']['prod']:
        return settings()['dbc_folders']['prod']
    elif local_repo_active_branch() == settings()['github_branches']['dev']:
        return settings()['dbc_folders']['dev']
    else:
        return settings()['dbc_folders']['branches'] + local_repo_active_branch() + "/"

def dbc_path(path):        return dbc_base() + path
def local_path(path):      return "./" + path + ".py"
def path_from_local(path): return path.replace("\\","/").replace(".py","").replace("./","")
def path_from_dbc(path):   return path.replace(dbc_base(),"")

def dbc(endpoint, json, ignored_errors=[]):
    api_method = requests.get if endpoint in ['workspace/list','workspace/export','clusters/list-zones','clusters/spark-versions'] else requests.post
    if os.environ.get('DBC_USER'):
        res = api_method(settings()['api_url'] + endpoint, json=json, auth=(os.environ['DBC_USER'],os.environ['DBC_PASS']))
    else:
        res = api_method(settings()['api_url'] + endpoint, json=json)
    if res.status_code == 401:
        raise click.ClickException("Unauthorized call to Databricks (remember to create a netrc file to authenticate with DBC - and check if your user/pass is the one used for DBC)")
    if res.status_code == requests.codes.ok or res.json()["error_code"] in ignored_errors:
        return res.json()
    else:
        raise click.ClickException(res.text)

def list_dbc_notebooks(path=None):
    if path == None:
        path = dbc_path("")
        click.echo("Listing dbc notebooks in " + dbc_path(""))
    res = dbc('workspace/list',json={ 'path': path }, ignored_errors=['RESOURCE_DOES_NOT_EXIST'])
    if "objects" not in res.keys():
        return []
    else:
        contents = res["objects"]

        folders   = [x['path']                for x in contents if x['object_type']=='DIRECTORY']
        notebooks = [path_from_dbc(x['path']) for x in contents if x['object_type']=='NOTEBOOK']
        for folder in folders:
            notebooks += list_dbc_notebooks(folder)
        return notebooks

def list_local_notebooks():
    click.echo("Listing local notebooks in current folder")
    notebooks = []
    for root, dirnames, filenames in os.walk('.'):
            for filename in filenames:
                if filename.endswith(".py") and not root.startswith("./."):
                    notebooks.append(path_from_local(os.path.join(root, filename)))
    return notebooks

def compare_repos():
    local_notebooks = set(list_local_notebooks())
    dbc_notebooks = set(list_dbc_notebooks())

    dbc_has_envfile = True if settings()['dbc_envfile_path'] in dbc_notebooks else False

    dbc_notebooks.discard(settings()['dbc_envfile_path'])
    local_notebooks.discard(settings()['dbc_envfile_path'])

    only_dbc = sorted(list(dbc_notebooks - local_notebooks))
    only_local = sorted(list(local_notebooks - dbc_notebooks))
    both = sorted(list(dbc_notebooks & local_notebooks))

    return only_dbc, only_local, both, dbc_has_envfile

def delete_local_notebook(path):
    click.echo("Deleting local notebook " + path)
    os.unlink(local_path(path))

def download_notebook(path):
    click.echo("Downloading notebook " + path)
    content = base64.b64decode( dbc('workspace/export',json={ 'format': 'SOURCE', 'path': dbc_path(path) })["content"] )

    dirpath = os.path.dirname(local_path(path))
    if dirpath != "" and not os.path.exists(dirpath):
        logging.debug("Creating directory path " + dirpath)
        try:
            os.makedirs(dirpath)
        except OSError as exc:
            if exc.errno != errno.EEXIST: raise

    with open(local_path(path), 'wb') as f: f.write(content)

def delete_dbc_notebook(path):
    click.echo("Deleting DBC notebook " + path)
    dbc('workspace/delete',json={ 'path': dbc_path(path) }, ignored_errors=["RESOURCE_DOES_NOT_EXIST"])

def upload_notebook(path):
    click.echo("Uploading notebook " + path)
    dbc('workspace/mkdirs', json={ 'path': os.path.dirname(dbc_path(path)) })

    with open(local_path(path), 'r') as f: content = base64.b64encode(f.read())
    dbc('workspace/import', json={ 'path':      dbc_path(path)
                        ,'language':  'PYTHON'
                        ,'overwrite': True
                        ,'content':   content
                        })

def clone_env_file():
    envfilename = "env_prod" if local_repo_active_branch() == settings()['github_branches']['prod'] else "env_dev"
    click.echo("Cloning env file from " + envfilename + " since there isnt one already")
    content = dbc('workspace/export',json={ 'format': 'SOURCE', 'path': settings()['dbc_folders']['envfiles'] + envfilename })["content"]
    dbc('workspace/import', json={ 'path':      dbc_path("_funksjoner/env")
                        ,'language':  'PYTHON'
                        ,'overwrite': True
                        ,'content':   content
                        })

@click.group()
def cli():
    """
    Utility to sync the local filesystem with DBC. Can only do one way at a time, either up or down.
    It deletes any notebooks in the target not present in the source, and copies all source notebooks
    to the target, overwriting any existing notebooks.

    The local folder is always the current folder bricker is run from.

    The DBC folder is chosen based on the current local git branch (a confirmation is need to push to prod).
    """
    click.echo("")
    try:
        settings()
    except IOError:
        raise click.ClickException("No bricker.yml file found.")

@cli.command()
def compare():
    """Only compares which notebooks are where"""
    only_dbc, only_local, both, dbc_has_envfile = compare_repos()
    print "\nNotebooks both local and in DBC: \n" + "\n".join(both)
    print "\nNotebooks only in DBC: \n" + "\n".join(only_dbc)
    print "\nNotebooks only local:  \n" + "\n".join(only_local)
    print "\nDBC has envfile: " + ("Yes" if dbc_has_envfile else "No")

@cli.command()
def down():
    """Syncs DBC notebooks to local"""
    only_dbc, only_local, both, dbc_has_envfile = compare_repos()
    if len(only_dbc+both) == 0:
        raise click.ClickException("There's no content in DBC for this branch, can't fetch")
    if len(only_local) > 10:
        if click.confirm('About to delete {} local files since they are not in dbc ({}). Are you sure this is what you want?'.format(len(only_local),", ".join(only_local))):
            click.echo("Aye aye cap'n!")
        else:
            click.echo("No problem - aborting")
            return

    p = Pool(10) # Running the web requests in parallell to speed stuff up
    p.map(download_notebook, (both + only_dbc))

    map(delete_local_notebook, only_local)

    click.echo("Staging all changes")
    local_repo().git.add(A=True)

@cli.command()
@click.option('--force', is_flag=True)
def up(force):
    """Syncs local notebooks to DBC"""
    only_dbc, only_local, both, dbc_has_envfile = compare_repos()
    if len(only_local+both) == 0:
        raise click.ClickException("There's no content locally, can't send to DBC")

    if local_repo_active_branch() == settings()['github_branches']['prod'] and not force:
        if click.confirm('You are in the prod branch. Are you sure you want to push to production?'):
            click.echo('Okay then... Fingers crossed :)')
        else:
            click.echo("No problem - aborting")
            return

    p = Pool(10)

    # Uploading with no parallellization due to race condition issues in DBC
    map(upload_notebook, (both + only_local))
    p.map(delete_dbc_notebook, only_dbc)

    if not dbc_has_envfile:
        clone_env_file()

@cli.command()
@click.option('--cluster_name', default=None)
@click.option('--num_workers', default=None)
def create_cluster(cluster_name,num_workers):
    """Creates cluster in Databricks using settings specified bricker.yml"""

    cluster_settings = settings()['default_cluster_settings']
    if cluster_name: cluster_settings["cluster_name"] = cluster_name
    if num_workers: cluster_settings["num_workers"] = num_workers

    click.echo("Creating cluster")
    click.echo(dbc('clusters/create',json= cluster_settings ))

if __name__ == '__main__':
    cli()
