import click
import pandas as pd
from os import mkdir, walk 
from os.path import exists, join

@click.command()
@click.option("--source1", prompt="source csv file", help="full path of source csv file")
@click.option("--source2", prompt="source csv file 2", help="full path 2 of source csv file")
def process(source1, source2):
    source1 = '/home/kzhu/.zipline/data/store/20210225'
    source2 = '/home/kzhu/.zipline/yahoo/20210301'
    print(f'source1 path {source1}')
    print(f'source2 path {source2}')
    if not exists(source1):
        print(f'the path {source1} does not exist')
        return
    if not exists(source2):
        print(f'the path {source2} does not exist')
        return
    result_source = source1 + '.merged'
    if not exists(result_source):
        mkdir(result_source)
    
    for root,d_names,f_names in walk(source1):
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
                    exist_file_name2 = join(source2, d_name, f_name)
                    new_file_name = join(result_source, d_name, f_name)
                    print(f'old {exist_file_name}, old2 {exist_file_name2}, new {new_file_name}')
                    in_data = pd.read_csv(exist_file_name, index_col=0)
                    in_data2 = pd.read_csv(exist_file_name2, index_col=0)
                    out_data = in_data.append(in_data2)
                    out_data.sort_index(inplace=True)
                    # out_data = in_data.drop(dropIndex)

                    out_data.to_csv(new_file_name)




if __name__ == '__main__':
    process()