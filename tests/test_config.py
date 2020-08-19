import os

potential_paths = [
    'tests/',
    '../tests/'
    'tap-square/tests/',
    '../tap-square/tests/',
]

def go_to_tests_directory():
    for path in potential_paths:
        if os.path.exists(path):
            os.chdir(path)
            return os.getcwd()
    raise NotImplementedError("This check cannot run from {}".format(os.getcwd()))

##########################################################################
### TEST
##########################################################################

print("Acquiring path to tests direcotry.")
cwd = go_to_tests_directory()

print("Reading in filenames from tests directory.")
files_in_dir = [name for name in os.listdir(cwd)]

print("Dropping files that are not of the form 'test_<feature>.py'.")
test_files_in_dir = [fn for fn in files_in_dir if 'test' == fn[:4] and '.py' == fn[-3:]]

print("Dropping test_client.py from test files.")
test_files_in_dir.remove('test_client.py')

print("Files found: {}".format(test_files_in_dir))

print("Reading contents of circle config.")
with open(cwd + "/../.circleci/config.yml", "r") as config:
    contents = config.read()

print("Parsing circle config for run blocks.")
runs = contents.replace(' ', '').replace('\n', '').split('-run:')

print("Verify all test files are executed in circle...")
file_found = {f: False for f in test_files_in_dir}
for filename in file_found.keys():
    print("\tVerifying {} is running in circle.".format(filename))
    if any([filename in run for run in runs]):
        file_found[filename] = True

assert all(file_found.values()), "The following tests are not running in circle:\t{}".format([k for k, v in file_found.items() if not v])
print("\t SUCCESS: All tests are running in circle.")
