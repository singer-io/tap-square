import os
import re
import os.path

# Read in the filenames of the tests/ directroy
cwd = os.getcwd()
print("contents of cwd: {}".format(os.listdir(cwd)))
print("contents of tests: {}".format(os.listdir(cwd + '/tests')))
print("contents of tap-square: {}".format(os.listdir(cwd + '/tap-square')))
files = [
    name for name in os.listdir('/tap-square/tests/')
    if 'test' == name[:4] and '.py' == name[-3:] and \
    name not in ['test_client.py', 'test_config.py']
]

# Read in the circle config
with open("../tap-square/.circleci/config.yml", "r") as config:
    contents = config.read()
runs = contents.replace(' ', '').replace('\n', '').split('-run:') # separate into run blocks

# Check that each file in the directory can be found in a run black in the circle config
matches = {f: False for f in files}
for m in matches.keys():
    if any([m in run for run in runs]):
        matches[m] = True

# Verify all files were found
if all(matches.values()):
    print("\t SUCCESS: All tests are running in circle.")
else:
    raise NotImplementedError("The following tests are not running in circle:\t{}".format([k for k, v in matches.items() if not v]))
