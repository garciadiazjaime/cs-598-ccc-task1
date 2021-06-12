from os import listdir
from os.path import isfile, join

from zipfile import ZipFile


def get_zip_files(path):
  files = [f for f in listdir(path) if isfile(join(path, f))]
  return files

def unzip_file(path):
  with ZipFile(path, 'r') as zipObj:
    zipObj.extractall()

def main():
  path = "./aviation/airline_origin_destination"
  files = get_zip_files(path)

  for file in files:
    unzip_file(path + '/' + file)

if __name__ == "__main__":
  main()
