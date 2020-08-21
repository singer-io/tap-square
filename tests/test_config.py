import os
import yaml

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

print("Acquiring path to tests directory.")
cwd = go_to_tests_directory()

print("Reading in filenames from tests directory.")
files_in_dir = [name for name in os.listdir(cwd)]

print("Dropping files that are not of the form 'test_<feature>.py'.")
test_files_in_dir = [fn for fn in files_in_dir if fn.startswith('test_') and fn.endswith('.py')]

print("Dropping test_client.py from test files.")
test_files_in_dir.remove('test_client.py')

print("Files found: {}".format(test_files_in_dir))

print("Reading contents of circle config.")
with open(cwd + "/../.circleci/config.yml", "r") as config:
    contents = config.read()

print("Parsing circle config for run blocks.")
run_tests = {
    step['run_test']['file']
    for step
    in yaml.load(contents, Loader=yaml.FullLoader)['jobs']['test']['steps']
    if isinstance(step, dict) and step.get('run_test')}

print("Verify all test files are executed in circle...")
discovered_tests = {test_file_in_dir.replace("test_", "").replace(".py", "")
                    for test_file_in_dir
                    in test_files_in_dir
                    # Can't watch the watchmen
                    if test_file_in_dir != "test_config.py"}

tests_not_found = discovered_tests - run_tests
assert tests_not_found == set(), "The following tests are not running in circle:\t{}".format(tests_not_found)
print("\t SUCCESS: All tests are running in circle.")
