from os import listdir, makedirs
from os.path import isfile, join
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

def main():
  path_from = "./aviation/airline_origin_destination"
  path_to = "./data/airline_origin_destination"
  files = get_zip_files(path_from)

  check_folder(path_to)

  for file in files:
    unzip_file(path_from, file, path_to)

if __name__ == "__main__":
  main()
