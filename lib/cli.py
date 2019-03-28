import click
import os 

@click.group()
def cli():
    pass

@cli.group()
def recipe():
    pass

@recipe.command('ls')
def list_recipes():
    for i in os.listdir('./recipes/'):
        click.echo(i)


# def get_recipes(ctx, args, incomplete):
#     return [k for k in os.listdir('./recipes/') if incomplete in k]

# @recipe.command('run')
# @click.argument('recipe', type=click.STRING, autocompletion=get_recipes)
# def run_recipes(recipe):
#     os.system(f'python ./recipes/{recipe}/build.py')



# def get_recipes(ctx, args, incomplete):
#     return [k for k in os.listdir('./recipes/') if incomplete in k]

@recipe.command('run')
@click.argument('recipe')
def run_recipes(recipe):
    os.system(f'python ./recipes/{recipe}/build.py')