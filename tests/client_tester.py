"""
This is used for testing basic functionality of the test client.
To run change the desired flags below and use the following command from the tap-tester repo:
  'python ../tap-adroll/tests/client_tester.py'
"""
from datetime import datetime
import random

from test_client import TestClient


##########################################################################
# Testing the TestCLient
##########################################################################
if __name__ == "__main__":
    client = TestClient(env='sandbox')
    # START_DATE = '2020-06-24T00:00:00Z'
    START_DATE = datetime.strftime(datetime.utcnow(), '%Y-%m-%dT00:00:00Z')

    # CHANGE FLAGS HERE TO TEST SPECIFIC FUNCTION TYPES
    test_creates = True
    test_updates = False  # To test updates, must also test creates
    test_gets = False
    test_deletes = True  # To test deletes, must also test creates

    # CHANGE FLAG TO PRINT ALL OBJECTS THAT FUNCTIONS INTERACT WITH
    print_objects = True

    objects_to_test = [ # CHANGE TO TEST DESIRED STREAMS
        # 'modifier_lists', # GET - DONE | CREATE -  | UPDATE -  | DELETE -
        # 'inventories', # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'items',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        'categories',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE - DONE
        # 'discounts',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'taxes',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'roles',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'employees',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'locations',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'payments',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'refunds',  # GET - DONE | CREATE - DONE | UPDATE - NA  | DELETE -
        # 'orders'  # GET - DONE | CREATE - DONE | UPDATE - DONE  | DELETE -
        # 'shifts'  # GET - DONE | CREATE - DONE | UPDATE - DONE  | DELETE -
    ]
    print("********** Testing basic functions of test client **********")
    if test_gets:
        for obj in objects_to_test:
            print("Testing GET (all): {}".format(obj))
            # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
            existing_obj = client.get_all(obj, START_DATE)
            if existing_obj:
                print("SUCCESS")
                if print_objects:
                    print("{}\n{}\n".format(existing_obj, len(existing_obj)))
                continue
            print("FAILED")

    if test_creates:
        for _ in range(1):
            for obj in objects_to_test:
                print("Testing CREATE: {}".format(obj))
                # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
                if obj == 'refunds':
                    payments = client.get_all('payments', start_date=START_DATE)
                created_obj = client.create(obj, start_date=START_DATE)
                if not created_obj:
                    print("FAILED")
                    continue
                print("SUCCESS")
                if print_objects:
                    print("{}\n".format(created_obj))

                if test_updates: # Need to reference specific attr from an obj to update
                    print("Testing UPDATE: {}".format(obj))
                    # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
                    obj_id = created_obj[0].get('id')
                    version = created_obj[0].get('version')
                    updated_obj = client.update(stream=obj, obj_id=obj_id, version=version,
                                                obj=created_obj[0], start_date=START_DATE)
                    if updated_obj:
                        print("SUCCESS")
                        if print_objects:
                            print("{}\n".format(updated_obj))
                        continue
                    print("FAILED")

                if test_deletes: # Need to reference id from an obj to delete
                    print("Testing UPDATE: {}".format(obj))
                    # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
                    obj_ids = [created_obj[0].get('id')]
                    version = created_obj[0].get('version')
                    deleted_obj = client.delete_catalog(ids_to_delete=obj_ids)
                    if deleted_obj:
                        print("SUCCESS")
                        if print_objects:
                            print("{}\n".format(deleted_obj))
                        continue
                    print("FAILED")
