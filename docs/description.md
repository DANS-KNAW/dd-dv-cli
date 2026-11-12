Description
===========

Dataverse command-line interface. It uses the [Dataverse API]{:target=_blank} and in some cases direct access to the Dataverse database. The commands are
implemented using [dans-dataverse-client-lib]{:target=_blank}.

dv dataset-edit-metadata
------------------------
For bulk changes on the metadata of multiple datasets in a Dataverse instance, you can use `dv dataset-edit-metadata`. This command uses the
[Edit metadata]{:target=_blank} endpoint, which allows you to add or replace metadata fields in a dataset. For typical usage, the command takes a CSV file as
input.

### Columns

The columns fall into several categories:

* Dataset identifier
    * `datasetId` - the database ID or PID of the dataset, a PID must contain the protocol, e.g., `doi:10.5072/FK2/123456`
    * `pid` - the persistent identifier of the dataset, which must start with a protocol label, e.g., `doi:10.5072/FK2/123456`
    * `databaseId` - the database ID of the dataset, consisting of digits only

      The input file must contain exactly one of these three columns (column names are case-insensitive); they are mutually exclusive. Each row must have a
      value in that column; there is no default identifier (the `--datasetId`, `--pid` and `--databaseId` options cannot be used together with an input file).
      Use `pid` or `databaseId` instead of `datasetId` if you want
      the identifier to be validated against its expected format.

* Preconditions
    * `expectedState` - the expected state of the dataset version to be edited, one of `draft`, `released`, `deacessioned` or a negation of one of those, e.g.,
      `not released`
    * `expectInReview` - whether the dataset is expected to be in review: `yes` or `no`
* Processing instructions
    * `publishVersion` - whether to publish the dataset after editing, one of `minor`, `major` or `leave-draft` (no
      publication action is attempted) or `no` (a synonym for `leave-draft`).
    * `versionNote` - a note to set on the draft version before publication. This involves a separate API call and requires the
      version note to be enabled in Dataverse. (See [Enabling version notes]{:target="_blank"}.)
    * `replace` - whether to replace existing values or add to them: `yes` or `no`
* Field values
    * An empty cell leaves the field unchanged if `replace` is `no`, and removes the field's value(s) if `replace` is `yes`.
    * To leave a field unchanged while `replace` is `yes`, put `!skip` in the cell. If the value `!skip` must literally be stored in a field, choose another
      skip
      code with the `--skip-code` option.

Column names are case-insensitive.

!!! note "Avoiding conflicts between special columns and fields"

    If a metadata field has the same name as one of the non-field columns above (e.g., `pid`), prefix the name of the non-field column with `*`, e.g.,
    `*pid`. The prefix is optional otherwise. If both `*pid` and `pid` are present, the first is the dataset's PID and the second is a metadata field. 








[Enabling version notes]: https://guides.dataverse.org/en/latest/installation/config.html#dataverse-feature-enable-version-note

### Field types

The names of the field value columns depend on the metadata blocks that are available.

* A primitive field or controlled vocabulary field is specified simply by the field identifier, e.g., `myField`.
* If multiple values are to be added, the field identifier is indexed, e.g., `myField[1]`. `myField[2]`, etc.
* A compound field is specified by the field identifier of the parent field, followed by a dot and the name of the child field, e.g., `myField.myChildField`.
  Note that in many cases the name of the child field refers to the parent field by convention, e.g., `authorName`, but you still have to prefix it with the
  parent field identifier, e.g., `author.authorName`.
* If multiple values of a compound field are to be added, the parent field identifier is indexed, e.g., `myField[1].myChildField`, `myField[2].myChildField`,
  etc.

### Default values

The command line options `--expectedState`, `--expectInReview`, `--publishVersion`, `--versionNote` and `--replace` can be combined with
`--input-file` (`-i`). In that case they provide default values for the corresponding columns:

* If the CSV file has no column of that name, the command line value is used for every row.
* If the column exists, the command line value is used for the rows where the cell is empty or blank. A non-blank cell always takes precedence.
* If neither a column value nor a command line option is given, the setting is not applied (e.g., no state expectation, no publication, no version note).

Field values have no defaults. When an input file is used, field values can only be provided in the file; specifying `<column name>=<value>` arguments on the
command line together with `--input-file` is an error. Blank and skip-code cells are handled as described above.

### Other options

The option `--timeout` (for example `30s`, `2m`; seconds if no unit is given) is not a column; it applies to the wait for publication of each dataset. The
option `--wait-between-rows` (same format as `--timeout`, default no wait; only allowed together with `--input-file`) is not a column either; it sets the time
to wait after each processed row, except the
last one, e.g., to throttle the load on the Dataverse instance. The
option `--skip-code` (default `!skip`) is not a column either; it sets the cell value that marks a field as not to be changed.

### Report

When running with a CSV input file, a report will be written to a file in the directory configured in `editMetadata.reportsDir` in `.dv.yml`. The
`--reports-dir` option can be used to override this location. The report contains a copy of each line, with the column names exactly as in the input (so an
optional `*` is kept or absent as it was), plus the extra columns `*result` and `*message`, which always have the `*`. When running without a CSV input file,
the report is written to the standard output.

The result column contains one of:

- `OK`: the row was processed successfully
- `FAILED`: an error occurred while processing the row
- `SKIPPED`: the row was skipped because a precondition was not met

### Running without a CSV input file

When testing, it can be useful to specify a row directly on the command-line, without going to the trouble of first saving it into a CSV file. The columns for
dataset identifier, preconditions and processing instructions all have their command line equivalent, e.g., `--datasetId`, `--pid`, `--databaseId` (mutually
exclusive, and exactly one is required when no input file is used), `--expectedState`, etc. Field values
can be specified on the command line as trailing arguments (only when no input file is used) in the format `<column name>=<value>`. Note that you will need to
enclose the whole argument in
quotes if the value contains spaces, or if the name has an index, e.g., `'myField[1]=longer value with spaces'`.

[Dataverse API]: {{ dataverse_api_url }}
[dans-dataverse-client-lib]: {{ dans_dataverse_client_lib_url }}
[Edit metadata]: {{ edit_metadata_url }}