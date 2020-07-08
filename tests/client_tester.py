"""
This is used for testing basic functionality of the test client.
To run change the desired flags below and use the following command from the tap-tester repo:
  'python ../tap-adroll/tests/client_tester.py'
"""
from test_client import TestClient


##########################################################################
# Testing the TestCLient
##########################################################################
if __name__ == "__main__":
    client = TestClient()
    START_DATE = '2020-06-24T00:00:00Z'

    # CHANGE FLAGS HERE TO TEST SPECIFIC FUNCTION TYPES
    test_creates = False
    test_updates = False # To test updates, must also test creates
    test_gets = True
    test_deletes = False

    # CHANGE FLAG TO PRINT ALL OBJECTS THAT FUNCTIONS INTERACT WITH
    print_objects = True

    objects_to_test = [ # CHANGE TO TEST DESIRED STREAMS 
        # 'items',  # GET - DONE | CREATE - DONE | UPDATE - DONE
        # 'categories',  # GET - DONE | CREATE - DONE | UPDATE - DONE
        # 'discounts',  # GET - DONE | CREATE - DONE | UPDATE - DONE
        # 'taxes',  # GET - DONE | CREATE - DONE | UPDATE - DONE
        # 'employees',  # GET - DONE | CREATE -  | UPDATE - 
        'locations',  # GET - DONE | CREATE - DONE | UPDATE - DONE
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
                    print("{}\n".format(existing_obj))
                continue
            print("FAILED")
    if test_creates:
        for obj in objects_to_test:
            print("Testing CREATE: {}".format(obj))
            # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
            created_obj = client.create(obj)
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
