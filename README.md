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
    * Customers

* Includes a schema for each resource reflecting most recent tested data retrieved using the api. See [the schema folder](https://github.com/singer-io/tap-square/tree/master/tap_square/schemas) for details.
* Some streams incrementally pull data based on the previously saved state. See the [bookmarking strategy](#bookmarking-strategy) section for more details.

## Bookmarking Strategy

The Square API for some objects supports a `begin_time` parameter that limits the query to only return objects with a `created_at` after the `begin_time`. For an example see [Square's list payments api documentation](https://developer.squareup.com/reference/square/payments-api/list-payments). Others support searching and sorting by custom fields and this tap bookmarks using the `updated_at` field in those cases. For example, see [Square's search orders api documentation](https://developer.squareup.com/reference/square/orders-api/search-orders).

Finally, some apis do not support incremental search, but to limit data volume output by the tap the tap queries for all and then filters out by the `updated_at` field. The [shifts stream](https://github.com/singer-io/tap-square/blob/42fbca80e22f0f292fa5202e9eaccb193ba7ea62/tap_square/streams.py#L207) is one such example.

Some apis allow sorting based on the `updated_at` value while others do not. If sorting is not supported one can only use the maximum `updated_at` value if and only if the stream has completly synced. To bookmark progress during the sync of a
stream the `cursor` value is saved and used to paginate through the API
(https://developer.squareup.com/docs/working-with-apis/pagination).

## Authentication

Authentication is handled with oauth v2. In the tap configuration the following fields are required for authentication to work correctly:

* client_id
* client_secret
* refresh_token

These values are all obtained from the oauth steps documented on [square's documentation page](https://developer.squareup.com/docs/build-basics/access-tokens#get-an-oauth-access-token). The url for oauth credentials once logged in should be something like `https://developer.squareup.com/apps/<app_id>/oauth`. The application id and secret refer to the client_id and client_secret values above. The refresh token is obtained by selecting `Authorize an Account` and then selecting the account.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    $ virtualenv -p python3 venv
    $ source venv/bin/activate
    $ pip install -e .
    ```
1. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-square <api_user_email@your_company.com>`
   - `sandbox` (string, optional): Whether to communication with square's sandbox or prod account for this application. If you're not sure leave out. Defaults to false.

   And the other values mentioned in [the authentication section above](#authentication).

    ```json
	{
		"client_id": "<app_id>",
		"start_date": "2020-08-21T00:00:00Z",
		"refresh_token": "<refresh_token>",
		"client_secret": "<app_secret>",
		"sandbox": "<true|false>",
		"user_agent": "Stitch Tap (+support@stitchdata.com)"
	}
	```

1. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-square --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    $ tap-square --config tap_config.json --catalog catalog.json >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    $ tap-square --config tap_config.json --catalog catalog.json | target-json >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    $ tap-square --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run >> state.json
    $ tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
---

Copyright &copy; 2020 Stitch
