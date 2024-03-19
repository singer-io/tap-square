# Changelog

## [v2.1.1] (2023-06-08)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v2.1.0...v2.1.1)

* Bugfix to replicate `payouts` stream records [#118](https://github.com/singer-io/tap-square/pull/118)

## [v2.1.0] (2023-06-08)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v2.0.0...v2.1.0)

* Replaces the deprecated `settlements` stream with the `payouts` stream [#109](https://github.com/singer-io/tap-square/pull/109)

## [v2.0.0](https://github.com/singer-io/tap-square/tree/v1.w.0) (2023-06-08)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.3.1...v2.0.0)

**API Version Upgrade:**

- Removed employees stream as it is deprecated and replaced it with the team_members stream [#111](https://github.com/singer-io/tap-square/pull/111)
- Schema Changes [#112](https://github.com/singer-io/tap-square/pull/112)

## [v1.3.1](https://github.com/singer-io/tap-square/tree/v1.w.0) (2021-06-10)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.3.0...v1.3.1)

* Removes `item_data` object from orders.json [#100](https://github.com/singer-io/tap-square/pull/100)

## [v1.3.0](https://github.com/singer-io/tap-square/tree/v1.w.0) (2021-01-21)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.2.0...v1.3.0)

* Update the schema for the `return_line_items` property under orders [#95](https://github.com/singer-io/tap-square/pull/95)

## [v1.2.0](https://github.com/singer-io/tap-square/tree/v1.2.0) (2020-12-31)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.1.0...v1.2.0)

**Merged pull requests:**

- run 3rd sync if customers stream needs it in bookmarks test [\#93](https://github.com/singer-io/tap-square/pull/93) ([kspeer825](https://github.com/kspeer825))
- Remove bookmark cursor from incremental streams and test bookmark cursor for every stream using it [\#92](https://github.com/singer-io/tap-square/pull/92) ([asaf-erlich](https://github.com/asaf-erlich))
- Refactor sync code into invidual stream classes [\#91](https://github.com/singer-io/tap-square/pull/91) ([asaf-erlich](https://github.com/asaf-erlich))
- stabilize bookmarks by removing customers from all assertions [\#90](https://github.com/singer-io/tap-square/pull/90) ([kspeer825](https://github.com/kspeer825))
- change cron time [\#89](https://github.com/singer-io/tap-square/pull/89) ([kspeer825](https://github.com/kspeer825))

## [v1.1.0](https://github.com/singer-io/tap-square/tree/v1.0.2) (2020-12-10)

* Fix customers stream to use date windowing to query [\#87](https://github.com/singer-io/tap-square/pull/84) ([cosimon](https://github.com/cosimon))

## [v1.0.2](https://github.com/singer-io/tap-square/tree/v1.0.2) (2020-12-02)

* Fix birthday, cursor pagination, and state saving [\#84](https://github.com/singer-io/tap-square/pull/84) ([cosimon](https://github.com/cosimon))
* Fix circle to stop tap-tester sharing PYTHONPATH with tap-square [\#85](https://github.com/singer-io/tap-square/pull/85) ([asaf-erlich](https://github.com/asaf-erlich))

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.0.1...v1.0.2)

## [v1.0.1](https://github.com/singer-io/tap-square/tree/v1.0.1) (2020-10-16)

* Fix the `all_fields_schema` test to not create Locations
* Removed unused schemas from `tap_square/schemas`
  * tap_square/schemas/agents.json
  * tap_square/schemas/companies.json
  * tap_square/schemas/contacts.json
  * tap_square/schemas/conversations.json
  * tap_square/schemas/groups.json
  * tap_square/schemas/satisfaction_ratings.json
  * tap_square/schemas/tickets.json

[Full Changelog](https://github.com/singer-io/tap-square/compare/v1.0.0...v1.0.1)

## [v1.0.0](https://github.com/singer-io/tap-square/tree/v1.0.0) (2020-09-30)

* Move tap-square to GA

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.7.1...v1.0.0)

## [v0.7.1](https://github.com/singer-io/tap-square/tree/v0.7.1) (2020-09-18)

* Add `cardholder_name` to `payments` schema

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.7.0...v0.7.1)

## [v0.7.0](https://github.com/singer-io/tap-square/tree/v0.7.0) (2020-09-08)

* Add customers stream

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.6.2...v0.7.0)

## [v0.6.2](https://github.com/singer-io/tap-square/tree/v0.6.2) (2020-06-22)

* Fixes Discovery in Sandbox to ignore streams unavailable in the sandbox
* Adds pagination to Settlements stream for time ranges over a year

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.6.0...v0.6.2)


## [v0.6.0](https://github.com/singer-io/tap-square/tree/v0.6.0) (2020-06-22)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.4.0...v0.6.0)

### Description

* More Preparation for beta testing

## [v0.5.0](https://github.com/singer-io/tap-square/tree/v0.5.0) (2020-06-22)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.4.0...v0.5.0)

### Description

* Preparation for beta testing

**Merged pull requests:**

- Update/readme [\#62](https://github.com/singer-io/tap-square/pull/62) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/payments all fields [\#60](https://github.com/singer-io/tap-square/pull/60) ([kspeer825](https://github.com/kspeer825))
- Remove spikes folder [\#59](https://github.com/singer-io/tap-square/pull/59) ([asaf-erlich](https://github.com/asaf-erlich))
- Cache forever get\_all\_location\_ids for a single tap sync job [\#58](https://github.com/singer-io/tap-square/pull/58) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/fix circle config [\#57](https://github.com/singer-io/tap-square/pull/57) ([kspeer825](https://github.com/kspeer825))
- wTesting/data diversity [\#56](https://github.com/singer-io/tap-square/pull/56) ([kspeer825](https://github.com/kspeer825))
- Missing fields in streams orders shifts + Fix start date test for all streams [\#55](https://github.com/singer-io/tap-square/pull/55) ([asaf-erlich](https://github.com/asaf-erlich))
- Added backoff library around client methods + Refactoring [\#54](https://github.com/singer-io/tap-square/pull/54) ([asaf-erlich](https://github.com/asaf-erlich))
- Fixing start\_date handling and added canary test to try to sync all streams [\#53](https://github.com/singer-io/tap-square/pull/53) ([dmosorast](https://github.com/dmosorast))
- Tests don't always create records unnecessarily [\#52](https://github.com/singer-io/tap-square/pull/52) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/running one by one [\#50](https://github.com/singer-io/tap-square/pull/50) ([kspeer825](https://github.com/kspeer825))
- Testing/cleanup [\#49](https://github.com/singer-io/tap-square/pull/49) ([kspeer825](https://github.com/kspeer825))
- Make created\_at and updated\_at keys that can be slightly off in the test [\#48](https://github.com/singer-io/tap-square/pull/48) ([asaf-erlich](https://github.com/asaf-erlich))
- test bookmarks prod and sandbox [\#47](https://github.com/singer-io/tap-square/pull/47) ([kspeer825](https://github.com/kspeer825))
- Added back inventories to test\_bookmarks and make fixes to the test to make it pass [\#46](https://github.com/singer-io/tap-square/pull/46) ([asaf-erlich](https://github.com/asaf-erlich))
- Add shifts to missing tests, now it's tested in all of them [\#45](https://github.com/singer-io/tap-square/pull/45) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/production [\#44](https://github.com/singer-io/tap-square/pull/44) ([kspeer825](https://github.com/kspeer825))
- Reduce duplicate code by having most v2 api calls use a shared method [\#43](https://github.com/singer-io/tap-square/pull/43) ([asaf-erlich](https://github.com/asaf-erlich))
- Shift does query by start time filtering, but that is only by the cre… [\#42](https://github.com/singer-io/tap-square/pull/42) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/mod lists pagination start date [\#41](https://github.com/singer-io/tap-square/pull/41) ([kspeer825](https://github.com/kspeer825))
- Making tests pass [\#40](https://github.com/singer-io/tap-square/pull/40) ([asaf-erlich](https://github.com/asaf-erlich))
- cleaned up all the merges, got sync working [\#39](https://github.com/singer-io/tap-square/pull/39) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add using batch create for the inventories - seconds to create 1000 records instead of minutes [\#38](https://github.com/singer-io/tap-square/pull/38) ([asaf-erlich](https://github.com/asaf-erlich))
- Square/modifier lists [\#37](https://github.com/singer-io/tap-square/pull/37) ([luandy64](https://github.com/luandy64))
- Add stream Cash Drawer Shifts [\#36](https://github.com/singer-io/tap-square/pull/36) ([luandy64](https://github.com/luandy64))
- Add stream Settlements [\#35](https://github.com/singer-io/tap-square/pull/35) ([luandy64](https://github.com/luandy64))
- Stream/v1 employee roles [\#34](https://github.com/singer-io/tap-square/pull/34) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add timecards [\#33](https://github.com/singer-io/tap-square/pull/33) ([luandy64](https://github.com/luandy64))
- Testing/inventory [\#32](https://github.com/singer-io/tap-square/pull/32) ([kspeer825](https://github.com/kspeer825))
- Stop skipping refunds and payments, adjusted some asserts [\#31](https://github.com/singer-io/tap-square/pull/31) ([luandy64](https://github.com/luandy64))
- Change payments to be full table stream since you can only query on created time but records can be updated [\#30](https://github.com/singer-io/tap-square/pull/30) ([asaf-erlich](https://github.com/asaf-erlich))
- Change refunds to be a full table stream since you can only query on created time but records can be updated [\#29](https://github.com/singer-io/tap-square/pull/29) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/bank accounts [\#28](https://github.com/singer-io/tap-square/pull/28) ([kspeer825](https://github.com/kspeer825))
- Add Orders [\#27](https://github.com/singer-io/tap-square/pull/27) ([luandy64](https://github.com/luandy64))
- Fix locations schema [\#26](https://github.com/singer-io/tap-square/pull/26) ([luandy64](https://github.com/luandy64))
- Add inventories stream [\#25](https://github.com/singer-io/tap-square/pull/25) ([asaf-erlich](https://github.com/asaf-erlich))
- Fix payments stream and test [\#24](https://github.com/singer-io/tap-square/pull/24) ([asaf-erlich](https://github.com/asaf-erlich))
- Testing/payments refunds [\#22](https://github.com/singer-io/tap-square/pull/22) ([kspeer825](https://github.com/kspeer825))
- added modifier lists, changed tests to accomodate nested comparisons [\#21](https://github.com/singer-io/tap-square/pull/21) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Testing/locations [\#20](https://github.com/singer-io/tap-square/pull/20) ([kspeer825](https://github.com/kspeer825))
- Square/refunds [\#19](https://github.com/singer-io/tap-square/pull/19) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Square/payments test [\#18](https://github.com/singer-io/tap-square/pull/18) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Square/debug [\#17](https://github.com/singer-io/tap-square/pull/17) ([kspeer825](https://github.com/kspeer825))
- Testing/employees [\#16](https://github.com/singer-io/tap-square/pull/16) ([kspeer825](https://github.com/kspeer825))
- Testing/add all fields test [\#15](https://github.com/singer-io/tap-square/pull/15) ([kspeer825](https://github.com/kspeer825))
- Testing/revise existing tests [\#14](https://github.com/singer-io/tap-square/pull/14) ([kspeer825](https://github.com/kspeer825))
- Square/tests [\#12](https://github.com/singer-io/tap-square/pull/12) ([jacobrobertbaca](https://github.com/jacobrobertbaca))

## [v0.4.0](https://github.com/singer-io/tap-square/tree/v0.4.0) (2020-06-22)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.3.0...v0.4.0)

**Merged pull requests:**

- Bump to v0.4.0, update changelog [\#11](https://github.com/singer-io/tap-square/pull/11) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add location [\#10](https://github.com/singer-io/tap-square/pull/10) ([jacobrobertbaca](https://github.com/jacobrobertbaca))

## [v0.3.0](https://github.com/singer-io/tap-square/tree/v0.3.0) (2020-06-22)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.2.0...v0.3.0)

**Merged pull requests:**

- Bump to v0.3.0, update changelog [\#9](https://github.com/singer-io/tap-square/pull/9) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add employee [\#8](https://github.com/singer-io/tap-square/pull/8) ([jacobrobertbaca](https://github.com/jacobrobertbaca))

## [v0.2.0](https://github.com/singer-io/tap-square/tree/v0.2.0) (2020-06-22)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.1.0...v0.2.0)

**Merged pull requests:**

- Bump to v0.2.0, update changelog [\#7](https://github.com/singer-io/tap-square/pull/7) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add tax [\#6](https://github.com/singer-io/tap-square/pull/6) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add sync for discounts [\#5](https://github.com/singer-io/tap-square/pull/5) ([jacobrobertbaca](https://github.com/jacobrobertbaca))
- Add categories [\#4](https://github.com/singer-io/tap-square/pull/4) ([jacobrobertbaca](https://github.com/jacobrobertbaca))

## [v0.1.0](https://github.com/singer-io/tap-square/tree/v0.1.0) (2020-06-12)

[Full Changelog](https://github.com/singer-io/tap-square/compare/v0.0.1...v0.1.0)

**Merged pull requests:**

- Version 0.1.0 [\#3](https://github.com/singer-io/tap-square/pull/3) ([luandy64](https://github.com/luandy64))
- Feature/sync catalog items [\#2](https://github.com/singer-io/tap-square/pull/2) ([luandy64](https://github.com/luandy64))

## [v0.0.1](https://github.com/singer-io/tap-square/tree/v0.0.1) (2020-06-10)

[Full Changelog](https://github.com/singer-io/tap-square/compare/b46489db5b3a94b113e142ef343cbd5dd1bb7542...v0.0.1)

**Merged pull requests:**

- Bump requests from 2.12.4 to 2.20.0 [\#1](https://github.com/singer-io/tap-square/pull/1) ([dependabot[bot]](https://github.com/apps/dependabot))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
