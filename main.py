import requests
from os import listdir, chdir, getcwd, path


def open_a_file(url):
    params = {
        'user.name': 'hadoop',
        'op': 'OPEN'
    }
    open_req = requests.get(url, params=params)
    print(open_req.text)


def mkdir(url):
    params = {
        'user.name': 'hadoop',
        'op': 'MKDIRS'
    }
    mk = requests.put(url, params=params)
    print(mk.text)
    return True


def put_a_file(url, local_path):
    params = {
        'user.name': 'hadoop',
        'op': 'CREATE',
        'noredirect': 'true'
    }
    put = requests.put(url, params=params)
    response = put.json()
    hdfs_url = response['Location']
    with open(local_path, 'r') as f:
        data = f.read()
    new_request = requests.put(hdfs_url, data=data)
    open_a_file(url)
    return True


def get_a_file(url, local_path):
    params = {
        'user.name': 'hadoop',
        'op': 'OPEN',
        'noredirect': 'true'
    }
    get = requests.get(url, params=params)
    response = get.json()
    hdfs_url = response['Location']
    new_request = requests.get(hdfs_url)
    with open(local_path, 'w') as f:
        f.write(new_request.text)
        print(new_request.text)
    return True


def append_a_file(url, local_path):
    params = {
        'user.name': 'hadoop',
        'op': 'APPEND',
        'noredirect': 'true'
    }
    with open(local_path, 'r') as f:
        data = f.read()
    append = requests.post(url, params=params)
    response = append.json()
    hdfs_url = response['Location']
    new_request = requests.post(hdfs_url, data=data)
    open_a_file(url)
    return True


def delete_a_file(url):
    params = {
        'user.name': 'hadoop',
        'op': 'DELETE'
    }
    delete = requests.delete(url, params=params)
    # print(delete.text)
    return True


def list_a_dir(url):
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
        if option == 'mkdir':
            mkdir(request_url)
        elif option == 'del':
            delete_a_file(request_url)
        elif option == 'ls':
            list_a_dir(request_url)
        elif option == 'cd':
            pwd = hdfs_path
            if inside[1] == '..':
                pwd = hdfs_path.split('/')
                pwd = pwd[:-2]
                pwd = '/'.join(pwd) + '/'
                print(pwd)
            else:
                pwd = pwd + '/'
                print(pwd)
        elif option == 'lcd':
            chdir(inside[1])
            print(f'{getcwd()}')
        else:
            print("Sorry, i don't know this command")
    elif len(inside) == 3:
        hdfs_path = pwd + inside[1]
        request_url = f'http://localhost:9870/webhdfs/v1{hdfs_path}'
        path_to_local = getcwd() + '/' + inside[2]
        # print('cdw', path_to_local)
        if option == 'put':
            put_a_file(request_url, path_to_local)
        elif option == 'get':
            get_a_file(request_url, path_to_local)
        elif option == 'app':
            append_a_file(request_url, path_to_local)
    else:
        hdfs_path = pwd
        request_url = f'http://localhost:9870/webhdfs/v1{hdfs_path}'
        # print('else path:', hdfs_path)
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
                    print('user', '   ', 'dir', '    ', elem)
                if elem in list(filter(path.isfile, listdir(getcwd()))):
                    print('user', '   ', 'file', '   ', elem)
        else:
            print("Sorry, i don't know this command")
    inside = ''.join(inside)
