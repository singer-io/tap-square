# tap-square

**This tap is in development.**

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Square's [REST API](https://developer.squareup.com/reference/square)
- Extracts the following resources from Square:
  - [Catalog](https://developer.squareup.com/reference/square/catalog-api)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Bookmarking Strategy

The Square API supports a `begin_time` parameter that limits the query to
only return objects with an `updated_at` after the `begin_time`. It does
not though allow sorting based on the `updated_at` value. This means we
can only use the maximum `updated_at` value if and only if we have
completly synced the stream. To bookmark our progress during the sync of a
stream we save the `cursor` value used to paginate through the API
(https://developer.squareup.com/docs/working-with-apis/pagination).

## TODO

- Extract additional endpoints from Square
- Build discount join table
- Persist updated refresh token

---

Copyright &copy; 2020 Stitch
