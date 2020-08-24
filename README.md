# tap-square

**This tap is in development.**

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Description
This tap:
* Downloads data from [Square API](https://developer.squareup.com/reference/square)
* Extracts from the following sources to produce [streams](https://github.com/singer-io/tap-square/blob/master/tap_square/streams.py). Below is a list of all the streams available. See the [streams file](https://github.com/singer-io/tap-square/blob/master/tap_square/streams.py) for a list of classes where each one has a constant indiciating if the stream's replication_method is INCREMENTAL or FULL_TABLE and what the replication_key is, usually `updated_at` field if it's incremental.
    * Items
    * Categories
    * Discounts
    * Taxes
    * Employees
    * Locations
    * BankAccounts
    * Refunds
    * Payments
    * ModifierLists
    * Inventories
    * Orders
    * Roles
    * Shifts
    * CashDrawerShifts
    * Settlements

* Includes a schema for each resource reflecting most recent tested data retrieved using the api. See [the schema folder](https://github.com/singer-io/tap-square/tree/master/tap_square/schemas) for details.
* Some streams incrementally pull data based on the previously saved state. See the [bookmarking strategy](#bookmarking-strategy) section for more details.

## Bookmarking Strategy

The Square API for some objects supports a `begin_time` parameter that limits the query to only return objects with a `created_at` after the `begin_time`. For an example see [Square's list payments api documentation](https://developer.squareup.com/reference/square/payments-api/list-payments). Others support searching and sorting by custom fields and this tap bookmarks using the `updated_at` field in those cases. For example, see [Square's search orders api documentation](https://developer.squareup.com/reference/square/orders-api/search-orders).

Finally, some apis do not support incremental search, but in order to keep backwards compatible features with sourcerer the tap queries for all and then filters out by the `updated_at`. The shifts stream is one such example.

Some apis allow sorting based on the `updated_at` value while others do not. If sorting is not supported one can only use the maximum `updated_at` value if and only if the stream has completly synced. To bookmark progress during the sync of a
stream the `cursor` value is saved and used to paginate through the API
(https://developer.squareup.com/docs/working-with-apis/pagination).

---

Copyright &copy; 2020 Stitch
