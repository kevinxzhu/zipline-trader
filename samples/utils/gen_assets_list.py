import click
import pandas as pd
from os import walk
from os.path import join, basename, splitext

@click.command()
@click.option("--source", prompt="source csv path", help="full path of source csv file")
def process(source):
    # print(f'source path {source}')
    
    for root,d_names,f_names in walk(source):
        # print(f'{root} {d_names} {f_names}')
        for d_name in d_names:
            folder = join(root, d_name)
            for root2,d_names2,f_names2 in walk(folder):
                # print(f'{root2} {d_names2} {f_names2}')
                for f_name in f_names2:
                    base = basename(f_name)
                    print(splitext(base)[0])

if __name__ == '__main__':
    process()