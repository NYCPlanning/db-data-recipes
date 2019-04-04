import click
import os 
from pathlib import Path
from lib.s3.make_client import make_client, get_last_modified

client = make_client()
bucket = os.environ.get('BUCKET')

@click.group()
def cli():
    pass

@cli.group()
def recipe():
    pass

def get_recipes(ctx, args, incomplete):
    return [k for k in os.listdir('./recipes/') if incomplete in k]

@recipe.command('ls')
@click.argument('recipe', type=click.STRING, autocompletion=get_recipes)
def list_recipes(recipe):
        if recipe == 'all':
                for i in os.listdir('./recipes/'):
                        click.echo(i)
        else:
                try: 
                        objs = client.list_objects_v2(Bucket=bucket, Prefix=recipe).get('Contents')
                        versions = set([Path(obj['Key']).parts[1] \
                                        for obj in sorted(objs, key=get_last_modified)])
                        click.echo('versions: ')
                        for i in versions: 
                                click.echo(i)
                except TypeError:
                        click.echo(f'\n WARNING: {recipe} is not present in S3 \n')

@recipe.command('run')
@click.argument('recipe', type=click.STRING, autocompletion=get_recipes)
def run_recipes(recipe):
    os.system(f'python ./recipes/{recipe}/build.py')