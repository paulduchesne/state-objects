
# script for safe storage of unique digital objects.

import datetime
import hashlib
import json
import pandas
import pathlib
import shutil

def checksummer(file_path):

    ''' Default MD5 checksumming function. '''

    with open(file_path, 'rb') as item:
        hash = hashlib.md5()
        for buff in iter(lambda: item.read(65536), b''):
            hash.update(buff)
        checksum = hash.hexdigest().lower()
        return(checksum)

def hash_extract(row, commence):

    ''' Process hash extract with expected processing time. '''

    status = (datetime.datetime.now()-commence)/(row.name+1)
    time_to_finish = (((status)*(len(dataframe)))+commence)
    time_to_finish = time_to_finish.strftime("%Y-%m-%d %H:%M:%S")
    print(f'hashing: {row.name+1} of {len(dataframe)}; eta {time_to_finish}.')
    return checksummer(row['FILE'])


def check_file(row, commence):

    ''' Check file existence and rehash matches. '''

    status = (datetime.datetime.now()-commence)/(row.name+1)
    time_to_finish = (((status)*(len(dataframe)))+commence)
    time_to_finish = time_to_finish.strftime("%Y-%m-%d %H:%M:%S")
    print(f'checking: {row.name+1} of {len(dataframe)}; eta {time_to_finish}.')

    file_ext = pathlib.Path(row['FILE']).suffix
    full_hash = row['HASH']
    hash_prefix = str(row['HASH'])[:2]
    predicted_file = out_dir / 'object' / hash_prefix / f'{full_hash}{file_ext}'
    if not pathlib.Path(predicted_file).exists():
        raise Exception(predicted_file, 'does not exist.')
    if full_hash != checksummer(predicted_file):
        raise Exception(predicted_file, 'hash not as expected.')
    return predicted_file    

# load config with defined source and target locations.

with open(pathlib.Path.cwd() / 'config.json') as config:
    config = json.load(config)
    in_dir = pathlib.Path(config['input_directory'])
    out_dir = pathlib.Path(config['output_directory'])

# check locations are mounted and/or accessible.

for x in [in_dir, out_dir]:
    if not x.exists():
        raise Exception(str(x), 'does not exist.')

# hash file list of target files.

file_list = [x for x in in_dir.glob('**/*') if x.is_file() == True]
dataframe = pandas.DataFrame(file_list, columns=['FILE'])
dataframe['HASH'] = dataframe.apply(hash_extract, commence=datetime.datetime.now(), axis=1)

# copy to drive if file does not exist.

if out_dir.exists():
    commence = datetime.datetime.now()
    for x in range(len(dataframe)):
        status = (datetime.datetime.now()-commence)/(x+1)
        time_to_finish = (((status)*(len(dataframe)))+commence)
        time_to_finish = time_to_finish.strftime("%Y-%m-%d %H:%M:%S")
        print(f'copying: {x+1} of {len(dataframe)}; eta {time_to_finish}.')

        item = dataframe.iloc[x]
        file = pathlib.Path(item['FILE'])
        hash = item['HASH']
        new_path = out_dir / 'object' / hash[:2] / f'{hash}{file.suffix}'
        new_path.parents[0].mkdir(exist_ok=True, parents=True)
        if not new_path.exists():
            shutil.copyfile(file, new_path)

# check files exist on target location and hash matches.

dataframe['SAFE'] = dataframe.apply(check_file, commence=datetime.datetime.now(), axis=1)

# write manifest report out.

timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
manifest_path = pathlib.Path(out_dir / 'manifest' / f'{timestamp}.csv')
manifest_path.parents[0].mkdir(exist_ok=True)
dataframe.to_csv(manifest_path, index='False')

print(len(dataframe))
print(len(dataframe.HASH.unique()))
