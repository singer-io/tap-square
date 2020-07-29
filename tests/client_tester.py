"""
This is used for testing basic functionality of the test client.
To run change the desired flags below and use the following command from the tap-tester repo:
  'python ../tap-adroll/tests/client_tester.py'
"""
import random

from test_client import TestClient


##########################################################################
# Testing the TestCLient
##########################################################################
if __name__ == "__main__":
    client = TestClient(env='sandbox')
    START_DATE = '2020-06-24T00:00:00Z'

    # CHANGE FLAGS HERE TO TEST SPECIFIC FUNCTION TYPES
    test_creates = True
    test_updates = False  # To test updates, must also test creates
    test_gets = True
    test_deletes = False  # To test updates, must also test creates

    # CHANGE FLAG TO PRINT ALL OBJECTS THAT FUNCTIONS INTERACT WITH
    print_objects = True

    objects_to_test = [ # CHANGE TO TEST DESIRED STREAMS 
        'modifier_lists', # GET - DONE | CREATE -  | UPDATE -  | DELETE - NA
        #'items',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE - NA
        # 'categories',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE - NA
        # 'discounts',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE - NA
        # 'taxes',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE - NA
        # 'employees',  # GET - DONE | CREATE -  | UPDATE -  | DELETE - NA
        'locations',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'payments',  # GET - DONE | CREATE - DONE | UPDATE - DONE | DELETE -
        # 'refunds',  # GET - DONE | CREATE - DONE | UPDATE - NA  | DELETE -
    ]
    print("********** Testing basic functions of test client **********")
    if test_gets:
        for obj in objects_to_test:
            print("Testing GET (all): {}".format(obj))
            import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
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
                import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
                ext_obj = None
                if obj == 'refunds':
                    payments = client.get_all('payments', start_date=START_DATE)
                created_obj = client.create(obj, ext_obj)
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
                    updated_obj = client.update(obj, obj_id=obj_id, version=version)
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
