# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [0.3.8] - 2021-05-24
### Changed
- Write functions to generate insert statements for values of map<text,text>, set<text> columns.

## [0.3.7] - 2021-05-23
### Changed
- In insert statement generation, escape single quotes for values of map<text,text>, set<text> columns.
- Accept port number when connecting to Cassandra.

## [0.3.6] - 2021-05-17
### Changed
- In insert statement generation, use the keyspace name along with the table name.

## [0.3.5] - 2021-05-14
### Changed
- Revert code to 0.3.0 and include only insert statement generation improvements (as the remaining changes are not working).

## [0.3.4] - 2021-05-14
### Changed
- Reset db related atom from core instead of from db.

## [0.3.3] - 2021-05-14
### Changed
- Make get-db-map public.

## [0.3.2] - 2021-05-14
### Changed
- Fix bug with de-referencing an atom.

## [0.3.1] - 2021-05-14
### Changed
- Fix insert statement generation bugs:
    - generate a boolean value as true instead of 'true'
    - generate an empty hash set value as {\'\'} instead of {\"\"}
    - generate a string value containing single-quotes with two single quotes i.e. Part(''ab'') instead of Part('ab')
    
## [0.3.0] - 2021-05-11
### Added
- `generate` namespace containing functions `get-insert-statements` & `get-insert-statement`.

## [0.2.1] - 2021-04-24
### Added
- `specs` namespace containing specs for other namespaces.

## [0.2.0] - 2021-03-29
### Added
- `analyze` namespace containing functions `get-counts` & `get-diff`.

## [0.1.0] - 2020-08-30
### Changed
- First version of this `sqlforcql` library with basic documentation.

[Unreleased]: https://github.com/your-name/sqlforcql/compare/0.1.1...HEAD
[0.1.1]: https://github.com/your-name/sqlforcql/compare/0.1.0...0.1.1
