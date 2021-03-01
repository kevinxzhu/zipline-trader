import click
import pandas as pd
from os import mkdir, walk 
from os.path import exists, join

@click.command()
@click.option("--source", prompt="source csv file", help="full path of source csv file")
@click.option("--dates", prompt="index[,index]", help="0-base index")
def hello(source, dates):
    print(f'source path {source}')
    print(f'will be dropped records date {dates}')
    if not exists(source):
        print(f'the path {source} does not exist')
        return
    result_source = source + '.new'
    if not exists(result_source):
        mkdir(result_source)
    
    dropIndex = [int(i) for i in dates.split(',')]
    for root,d_names,f_names in walk(source):
        # print(f'{root} {d_names} {f_names}')
        for d_name in d_names:
            folder = join(root, d_name)
            for root2,d_names2,f_names2 in walk(folder):
                # print(f'{root2} {d_names2} {f_names2}')
                new_folder = join(result_source, d_name)
                if not exists(new_folder):
                    mkdir(new_folder)
                for f_name in f_names2:
                    exist_file_name = join(folder, f_name)
                    new_file_name = join(result_source, d_name, f_name)
                    print(f'old {exist_file_name}, new {new_file_name}')
                    in_data = pd.read_csv(exist_file_name)

                    out_data = in_data.drop(dropIndex)

                    out_data.to_csv(new_file_name)




if __name__ == '__main__':
    hello()