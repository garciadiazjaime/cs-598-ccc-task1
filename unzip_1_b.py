from os import listdir, makedirs
from os.path import isfile, join, isdir
from zipfile import ZipFile


def get_zip_files(path):
  files = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith('.zip')]
  return files

def check_folder(path):
  makedirs(path, exist_ok=True)

def unzip_file(path_from, file, path_to):
  zipped_path = path_from + "/" + file
  with ZipFile(zipped_path, 'r') as zipObj:
    [name, _] = file.split(".")
    unzip_path = path_to + "/" + name
    check_folder(unzip_path)
    print("unziping: %s into %s" % (zipped_path, unzip_path))
    zipObj.extractall(unzip_path)

def get_folders(path):
  files = [f for f in listdir(path) if isdir(join(path, f))]
  return files

def main():
  path_from = "./aviation/airline_ontime"

  path_to = "./data/aviation/airline_ontime/"
  check_folder(path_to)

  folders = get_folders(path_from)
  for folder in folders:
    files = get_zip_files(path_from + "/" + folder)

    for file in files:
      unzip_file(path_from + "/" + folder, file, path_to)

if __name__ == "__main__":
  main()
