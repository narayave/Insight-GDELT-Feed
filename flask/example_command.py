import click
import time

@click.group()
def cli():
    'A stupid script to test click-web'
    pass

@cli.command()
@click.option("--delay", type=float, default=0.01, help='delay for every line print')
@click.argument("lines", default=10, type=int)
def print_rows(lines, delay):
    'Print lines with a delay'
    click.echo(f"writing: {lines} with {delay}")
    for i in range(lines):
        click.echo(f"Hello row: {i}")
        time.sleep(delay)

if __name__ == '__main__':
    cli()
