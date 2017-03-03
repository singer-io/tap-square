# tap-square

**This tap is in development.**

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Square's [REST API](https://docs.connect.squareup.com/api/connect/v1/)
- Extracts the following resources from Square:
  - [Locations]()
  - [Payments]()
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## TODO

- Extract additional endpoints from Square
- Build discount join table
- Persist updated refresh token

---

Copyright &copy; 2017 Stitch
