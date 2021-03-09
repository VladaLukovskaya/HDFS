import requests
from os import listdir, chdir, getcwd, path

# r = requests.get('http://localhost:9870', auth=('hadoop', 'hdoop$#'))


def mkdir(url):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'MKDIRS'
    }
    mk = requests.put(url, params=params)
    print(mk.text)
    return True


def put_a_file(url, local_path):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'CREATE'
    }
    # path_to_file = input('Path to the local file: ')
    put = requests.put(url, params=params, data=open(local_path, 'rb'))
    print(put.text)
    return True


def get_a_file(url, local_path):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'OPEN'
    }
    get = requests.get(url, params=params)
    with open(local_path, 'w') as f:
        f.write(get.text)
    print(get.text)
    return True


def append_a_file(url, local_path):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'APPEND'
    }
    with open(local_path, 'rb') as f:
        data = f.read()
    append = requests.post(url, params=params, data=data)
    print(append.text)
    return True


def delete_a_file(url):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'DELETE'
    }
    delete = requests.delete(url, params=params)
    print(delete.text)
    return True


def list_a_dir(url):  # done
    params = {
        'user.name': 'hadoop',
        'op': 'LISTSTATUS'
    }
    ls = requests.get(url, params)
    response = ls.json()
    status = response['FileStatuses']['FileStatus']
    empty_space = '   '
    for file in status:
        if file['type'] == 'DIRECTORY':
            print(file['length'], file['owner'], file['type'], file['pathSuffix'], sep='\t')
        else:
            print(file['length'], file['owner'], file['type'], empty_space, file['pathSuffix'], sep='\t')
    return True


inside = ''
hdfs_path = ''
pwd = '/'
while inside != 'exit':
    inside = input(f'hadoop@hdfs:{pwd}$ ')
    inside = inside.split(' ')
    option = inside[0]
    if len(inside) == 2:
        hdfs_path = pwd + inside[1]
        request_url = f'http://localhost:9870/webhdfs/v1{hdfs_path}'
        # print('path:', path)
        # print('req:', request_url)
        if option == 'mkdir':
            mkdir(request_url)
        elif option == 'del':
            delete_a_file(request_url)
        elif option == 'ls':
            list_a_dir(request_url)
        elif option == 'cd':
            pwd = path
            if inside[1] == '..':
                pwd = path.split('/')
                pwd = pwd[:-2]
                pwd = '/'.join(pwd) + '/'
        elif option == 'lcd':
            chdir(inside[1])
        else:
            print("Sorry, i don't know this command")
    elif len(inside) == 3:
        hdfs_path = pwd + inside[1]
        request_url = f'http://localhost:9870/webhdfs/v1{hdfs_path}'
        path_to_local = getcwd() + '/' + inside[2]
        print('cdw', path_to_local)
        if option == 'put':
            put_a_file(request_url, path_to_local)
        elif option == 'get':
            get_a_file(request_url, path_to_local)
        elif option == 'app':
            append_a_file(request_url, path_to_local)
    else:
        hdfs_path = pwd
        request_url = f'http://localhost:9870/webhdfs/v1{hdfs_path}'
        # print('else path:', path)
        # print('else req:', request_url)
        if option == 'ls':
            list_a_dir(request_url)
        elif option == 'exit':
            pass
        elif option == 'lls':
            print(f'  {getcwd()}:')
            lls_status = listdir(getcwd())
            for elem in lls_status:
                if elem in list(filter(path.isdir, listdir(getcwd()))):
                    print('dir', '    ', elem)
                if elem in list(filter(path.isfile, listdir(getcwd()))):
                    print('file', '   ', elem)
        else:
            print("Sorry, i don't know this command")
    inside = ''.join(inside)
