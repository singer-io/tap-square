from datetime import datetime as dt
from datetime import timedelta

from tap_square.streams import Locations, Orders
from base import TestSquareBase


class UnitTestLocations(TestSquareBase):
    """Run implementation-specific tests around the Locations stream"""
    def name(self):
        return "unittest_tap_square_locations"


    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))
    

    def test_run(self):
        print("\nTesting the 'get_all_location_ids method.\n")
        self.get_all_location_ids_test()
        self.location_orders_dependency_test()


    def location_orders_dependency_test():
        orders = Orders()
        pass

    def location_cds_dependency_test():
        """Can't test the locations dependency for cash_drawer_shifts since we cannot create test data."""
        pass

    def location_settlements_dependency_test():
        """Can't test the locations dependency for settlements since we cannot create test data."""
        pass

    def get_all_location_ids_test(self):
        """
        Verify the get_all_locations_ids helper method returns the same number of records as the standard get method.
        Verify this against various start dates
        """
        locations = Locations()
        today = dt.strftime(dt.utcnow(), self.START_DATE_FORMAT)
        start_dates = {"today": today}
        start_dates["three_days_ago"] = self.timedelta_formatted(today, days=-3)
        start_dates["one_month_ago"] = self.timedelta_formatted(today, days=-30)
        start_dates["one_year_ago"] = self.timedelta_formatted(today, days=-365)

        for description in start_dates:
            with self.subTest(description=description):
                start_date = start_dates.get(description)
                locations_from_test_client = self.client.get_all('locations', start_date) # results from test_client (Square Client get)
                locations_from_helper_method = locations.get_all_location_ids(self.client, start_date)  # results from the helper method
                print("Testing for a start date of {}: {}".format(description, start_date))
                print("TestClient returned {} records".format(len(locations_from_test_client)))
                print("Helper function returned {} records".format(len(locations_from_helper_method)))
                self.assertEqual(len(locations_from_test_client), len(locations_from_helper_method),
                                 msg="Mehtod failed with start date of {}: {}".format(description, start_date))
                print("Method passed with start date of {}: {}\n".format(description, start_date))
