import click
import os 

@click.group()
def cli():
    pass


@cli.command('ls')
@click.argument('stuff', type=str)
def list_stuff(stuff):
    if stuff == 'pipe': 
        click.echo(os.system('dpp'))
    elif stuff == 'datasets': 
        click.echo(os.system('ls ./recipes/'))

@cli.command('update')
@click.argument('dataset', type=str)
@click.option('--dumptopath', default='no', help='yes or no')
def update(dataset, dumptopath): 
    os.system(f'python ./recipes/{dataset}/build.py')
    click.echo(f'{dataset} update complete')