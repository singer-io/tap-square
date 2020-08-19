import os


cwd = os.getcwd()
if cwd not in {'root/project', '/opt/code/tap-square'}:
    print("WARN: This script is meant to run from the top level directory of the tap.")

print("Reading in filenames from tests directory.")
print("Parsing directory for tests.")
files = [
    name for name in os.listdir(cwd + '/tests')
    if 'test' == name[:4] and '.py' == name[-3:] and \
    name not in ['test_client.py', 'test_config.py']
]
print("\tFiles found: {}".format(files))

print("Reading contents of circle config")
with open(cwd + "/.circleci/config.yml", "r") as config:
    contents = config.read()

# Check that each file in the directory can be found in a run black in the circle config
print("Parsing circle config for run blocks.")
runs = contents.replace(' ', '').replace('\n', '').split('-run:') # separate into run blocks
matches = {f: False for f in files}
for m in matches.keys():
    print("Verifying {} is running in circle.".format(m))
    if any([m in run for run in runs]):
        matches[m] = True

# Verify all files were found
assert all(matches.values()), "The following tests are not running in circle:\t{}".format([k for k, v in matches.items() if not v])
print("\t SUCCESS: All tests are running in circle.")
