import click
import os 
from pathlib import Path
from lib.s3.make_client import make_client, get_last_modified
import emoji 

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
        if recipe == 'local':

                click.echo('\n')
                for i in os.listdir('./recipes/'):
                        click.secho(i, fg='yellow')
                click.echo('\n')

        elif recipe == 's3': 
                objs = client.list_objects_v2(Bucket=bucket).get('Contents')
                recipes = set([Path(obj['Key']).parts[0] for obj in objs])
                
                click.echo('\n')
                for i in recipes:
                        click.secho(i, fg='magenta')
                click.echo('\n')
                        
        elif recipe == 'diff': 
                objs = client.list_objects_v2(Bucket=bucket).get('Contents')
                in_s3 = set([Path(obj['Key']).parts[0] for obj in objs])
                in_local = set(os.listdir('./recipes/'))

                click.echo('\n')
                click.secho('in s3 not in local:', fg='green', bold=True)
                for i in in_s3.difference(in_local):
                        click.secho(i, fg='magenta')
                click.echo('\n')

                click.secho('in local not in s3:', fg='green', bold=True)
                for i in in_local.difference(in_s3):
                        click.secho(i, fg='yellow')
                click.echo('\n')
        else:
                try: 
                        objs = client.list_objects_v2(Bucket=bucket, Prefix=recipe).get('Contents')
                        versions = set([Path(obj['Key']).parts[1] \
                                        for obj in sorted(objs, key=get_last_modified)])
                        
                        click.echo('\n')
                        click.secho('versions:', fg='green', bold=True)
                        for i in versions: 
                                click.secho(i, fg='magenta')
                        click.echo('\n')

                except TypeError:
                        click.echo('\n')
                        click.secho(f'{recipe} is not present in S3', fg='red', bold=True)
                        click.echo(f'Do you want to run {recipe}? [y/n]', nl=False)
                        c = click.getchar()
                        if c == 'y': 
                                click.echo('\n')
                                click.echo(f'running {recipe} ...')
                                os.system(f'python ./recipes/{recipe}/build.py')
                                click.echo('\n')
                        elif c == 'n':
                                click.echo('Ok bye ...')
                        else:
                                click.echo(emoji.emojize('Invalid input :broken_heart:'))

@recipe.command('run')
@click.argument('recipe', type=click.STRING, autocompletion=get_recipes)
def run_recipes(recipe):
    os.system(f'python ./recipes/{recipe}/build.py')