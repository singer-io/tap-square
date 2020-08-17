import os

from unittest import TestCase

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import TestSquareBase


class TestSquarePayments(TestSquareBase, TestCase):
    """Test specific data input results in the expected output in the target"""

    def name(self):
        return "tap_tester_square_payments"

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

    def tested_stream(self):
        return 'payments'

    @classmethod
    def tearDownClass(cls):
        print("\n\nTEST TEARDOWN\n\n")

    def ensure_dict_object(self, resp_object):
        if isinstance(resp_object, dict):
            return resp_object
        elif isinstance(resp_object, list):
            self.assertEqual(1, len(resp_object),
                             msg="Multiple objects were returned, but only 1 was expected")
            self.assertTrue(isinstance(resp_object[0], dict),
                            msg="Response object is a list of {} types".format(type(resp_object[0])))
            return resp_object[0]
        else:
            raise RuntimeError("Type {} was unexpected.\nRecord: {} ".format(type(resp_object), resp_object))

    def test_run(self):
        """
        Verify that for payments stream you can get data.
        """
        stream = self.tested_stream()
        self.START_DATE = self.get_properties().get('start_date')

        # Generate various records for the tested stream
        created_records = {stream: dict()}

        # Create a payment using a card
        desc = "card"
        card_payment = self.ensure_dict_object(
            self.client._create_payment(source_key=desc)
        )
        created_records[stream][desc] = card_payment

        # Create a payment using a card on file
        desc = "card_on_file"
        card_on_file_payment = self.ensure_dict_object(
            self.client._create_payment(source_key=desc)
        )
        created_records[stream][desc] = card_on_file_payment

        # Create a payment using a gift card
        desc = "gift_card"
        gift_card_payment = self.ensure_dict_object(
            self.client._create_payment(source_key=desc)
        )
        created_records[stream][desc] = gift_card_payment

        # Create a payment that will autocomplete
        desc = "autocomplete"
        autocomplete_payment = self.ensure_dict_object(
            self.client._create_payment(autocomplete=True, source_key='card')
        )
        created_records[stream][desc] = autocomplete_payment

        # Track all fields from created records
        fields = set()
        for desc, record in created_records[stream].items():
            print("\n\nNew fields found: create of a {} {}".format(desc, stream))
            if set(record.keys()).difference(fields):
                print("Adding untracked fields to tracked set: {}\n\n".format(set(record.keys()).difference(fields)))
            fields.update(record.keys())
            
        # Update the records above
        updated_records = {stream: dict()}

        # Update a completed payment by making a refund (payments must have a status of 'COMPLETED' to process a  )
        created_desc = "autocomplete"
        updated_desc = "refund"
        card_refund, refunded_payment = self.client.create_refund(self.START_DATE, created_records[stream][created_desc])
        refunded_payment = self.ensure_dict_object(refunded_payment)
        updated_records[stream][updated_desc] = refunded_payment

        # Update a payment by completing it
        created_desc = "card_on_file"
        updated_desc = "complete"
        obj = created_records[stream][created_desc]
        completed_payment = self.ensure_dict_object(
            self.client._update_payment(obj.get('id'), obj=obj, action=updated_desc)
        )
        updated_records[stream][updated_desc] = completed_payment

        # Update a payment by canceling it
        created_desc = "gift_card"
        updated_desc = "cancel"
        obj = created_records[stream][created_desc]
        canceled_payment = self.ensure_dict_object(
            self.client._update_payment(obj.get('id'), obj=obj, action=updated_desc)
        )
        updated_records[stream][updated_desc] = canceled_payment

        # Submit a dispute for a completed payment
        # desc = "complete"
        # updated_desc = "dispute"
        # obj = updated_records[stream][desc]
        # import pdb; pdb.set_trace()
        # dispute_payment = self.ensure_dict_object(
        #     self.client._update_payment(obj.get('id'), obj=obj, action=updated_desc)
        # )
        # updated_records[stream][updated_desc] = canceled_payment

        # Track all fields from the updated records
        for desc, record in updated_records[stream].items():
            if set(record.keys()).difference(fields):
                print("\n\nNew fields found: update on a {} {}".format(desc, stream))
                print("Adding untracked fields to tracked set: {}\n\n".format(set(record.keys()).difference(fields)))
            fields.update(record.keys())

        # Verify all the fields found in the created and updated records are accounted for by the schema
        schema_keys = set(self.expected_schema_keys(stream))
        self.assertTrue(fields.issubset(schema_keys),
                        msg="Fields missing from schema: {}".format(fields.difference(schema_keys)))


if __name__ == '__main__':
    unittest.main()
