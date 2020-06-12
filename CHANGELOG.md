# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased][unreleased]
### Changed

## [0.1.0] - 2020-06-12
### Added
- `Items` as a syncable stream
- `sandbox` as a config parameter which if true will have the tap connect
  to a sandbox environment

### Removed
- Old code from previous test version of tap

## [0.0.1] - 2020-06-10
### Added
- Files from the new template.
- A spike to evaluate the use of [square-python-sdk][square-python-sdk]
- A spike to compare sync approaches in the SDK
  - Should we use a `sorted_attribute_query` to sort the results by
    `updated_at`
    - No, the query does not support sorting by that attribute.

[unreleased]: https://github.com/singer-io/tap-square/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/singer-io/tap-square/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/singer-io/tap-square/tree/v0.0.1


[square-python-sdk]: https://developer.squareup.com/docs/sdks/python
