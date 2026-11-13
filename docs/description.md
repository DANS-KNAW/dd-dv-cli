Description
===========

Dataverse command-line interface. It uses the [Dataverse API]{:target=_blank} and in some cases direct access to the Dataverse database. The commands are
implemented using [dans-dataverse-client-lib]{:target=_blank}. Some of the more complex commands are documented below.

Common features of `dv dataverse-create-dataset` and `dv dataset-edit-metadata`
-------------------------------------------------------------------------------
The commands [`dv dataverse-create-dataset`](#dv-dataverse-create-dataset) and [`dv dataset-edit-metadata`](#dv-dataset-edit-metadata) work in the same
way: they process one dataset specified on the command line, or a batch of datasets specified in a CSV input file (`--input-file`, `-i`), one row per dataset.
This section describes what they have in common; the sections on the individual commands describe their specific columns and options.

### Input file and columns

The columns of the input file fall into several categories. Which of these apply depends on the command:

* Dataset identifier (`dv dataset-edit-metadata` only)
* Preconditions (`dv dataset-edit-metadata` only)
* Processing instructions
* Field values

Column names are case-insensitive.

!!! note "Avoiding conflicts between special columns and fields"

    If a metadata field has the same name as one of the non-field columns of the command (e.g., `pid`), prefix the name of the non-field column with `*`, e.g.,
    `*pid`. The prefix is optional otherwise. If both `*pid` and `pid` are present, the first is the special column and the second is a metadata field. Field
    names must not start with `*`. The `*result` and `*message` columns in the [report](#report) always have the `*` preempting any possible conflicts with
    field names.

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

Values for the columns can also be given as command line options, which can be combined with `--input-file`. In that case they provide default values for
the corresponding columns:

* If the CSV file has no column of that name, the command line value is used for every row.
* If the column exists, the command line value is used for the rows where the cell is empty or blank. A non-blank cell always takes precedence.
* If neither a column value nor a command line option is given, the setting is not applied (unless the command defines a default for it).

Field values can have defaults too: `<column name>=<value>` arguments can be combined with `--input-file`. They work the same way: a column that is missing
from the CSV file gets the value for every row, and for an existing column the value is used for the rows where the cell is empty or blank. How blank cells in
field columns are interpreted otherwise is specific to the command.

### Other options

* `--timeout` (for example `30s`, `2m`; seconds if no unit is given) is not a column; it applies to the wait for publication of each dataset.
* `--wait-between-rows` (same format as `--timeout`, default no wait; only allowed together with `--input-file`) is not a column either; it sets the time to
  wait after each processed row, except the last one, e.g., to throttle the load on the Dataverse instance.
* `--reports-dir` overrides the location of the [report](#report).

### Report

When running with a CSV input file, a report will be written to a file in the directory configured in `editMetadata.reportsDir` in `.dv.yml`. The
`--reports-dir` option can be used to override this location. The report contains a copy of each line, with the column names exactly as in the input (so an
optional `*` is kept or absent as it was), plus the columns for default values that were not in the input file, so that the effective input is visible (blank
cells in the input are filled with the default value), plus the extra columns `*result` and `*message`, which always have the `*`. When running without a CSV
input file, the report is written to the standard output.

The `*result` column contains one of:

- `OK`: the row was processed successfully
- `FAILED`: an error occurred while processing the row
- `SKIPPED`: the row was skipped because a precondition was not met

### Running without a CSV input file

When testing, it can be useful to specify a row directly on the command-line, without going to the trouble of first saving it into a CSV file. The columns
have their command line equivalent, e.g., `--publishVersion`, `--expectedState`, etc. Field values can be specified on the command line as trailing arguments
in the format `<column name>=<value>`. Note that you will need to enclose the whole argument in quotes if the value contains spaces, or if the name has an
index, e.g., `'myField[1]=longer value with spaces'`.

dv dataverse-create-dataset
---------------------------
Creates one dataset, or a batch of datasets, in a collection. It uses the [Create a dataset]{:target=_blank} endpoint. The common features described
[above](#common-features-of-dv-dataverse-create-dataset-and-dv-dataset-edit-metadata) apply. Only the metadata blocks enabled for the target collection can be
used, and the fields that Dataverse requires must be provided.

The differences with [`dv dataset-edit-metadata`](#dv-dataset-edit-metadata) are:

* There is no dataset identifier: the persistent identifier is assigned by Dataverse and reported in the `*message` column of the [report](#report).
* There are no preconditions (`expectedState`, `expectInReview`, `=<fieldId>`), because the dataset does not exist yet.
* There is no `replace`, `versionNote` or skip code. An empty cell simply means that the field gets no value.

### Columns

* `alias` - the alias of the collection in which to create the dataset. Default: the value of `--alias`, or `root` if that is not specified either. A
  non-blank cell takes precedence over the command line option.
* `publish` - whether to publish the dataset after creation: `yes`, or `no` (synonym: `leave-draft`). Default: the value of `--publish`, or `no`. A newly
  created dataset is always published as version 1.0, so there is no choice of version type.
* Any other column is a metadata field value (see [Field types](#field-types)).

Example:

```bash
dv dataverse-create-dataset --alias mycollection --publish yes \
   'title=My dataset' 'author[1].authorName=Doe, Jane' 'subject[1]=Chemistry'
```

dv dataset-edit-metadata
------------------------
For bulk changes on the metadata of multiple datasets in a Dataverse instance, you can use `dv dataset-edit-metadata`. This command uses the
[Edit metadata]{:target=_blank} endpoint, which allows you to add or replace metadata fields in a dataset. For typical usage, the command takes a CSV file as
input. The common features described
[above](#common-features-of-dv-dataverse-create-dataset-and-dv-dataset-edit-metadata) apply.

### Columns

* Dataset identifier
    * `datasetId` - the database ID or PID of the dataset, a PID must contain the protocol, e.g., `doi:10.5072/FK2/123456`
    * `pid` - the persistent identifier of the dataset, which must start with a protocol label, e.g., `doi:10.5072/FK2/123456`
    * `databaseId` - the database ID of the dataset, consisting of digits only

      The input file must contain exactly one of these three columns; they are mutually exclusive. Each row must have a value in that column; there is no
      default identifier (the `--datasetId`, `--pid` and `--databaseId` options cannot be used together with an input file). Use `pid` or `databaseId` instead
      of `datasetId` if you want the identifier to be validated against its expected format. When no input file is used, exactly one of the three options is
      required.

* Preconditions
    * `expectedState` - the expected state of the dataset version to be edited, one of `draft`, `released`, `deacessioned` or a negation of one of those, e.g.,
      `not released`
    * `expectInReview` - whether the dataset is expected to be in review: `yes` or `no`
    * `=<fieldId>` - the expected current value of the field with identifier `<fieldId>`, e.g., `=title`, `=subject[2]` or `=author[1].authorName`. Before
      making any changes, the command checks that the field in the latest version has exactly this value (case-sensitive, surrounding whitespace in the cell
      ignored). If not, the row is skipped. An empty cell means the field is expected to have no value. For a multi-valued field without index, the
      first value is checked. On the command line, use e.g. `=title=Old title`.
* Processing instructions
    * `publishVersion` - whether to publish the dataset after editing, one of `minor`, `major` or `leave-draft` (no
      publication action is attempted) or `no` (a synonym for `leave-draft`).
    * `versionNote` - a note to set on the draft version before publication. This involves a separate API call and requires the
      version note to be enabled in Dataverse. (See [Enabling version notes]{:target="_blank"}.)
    * `replace` - whether to replace existing values or add to them: `yes` or `no`
* Field values
    * An empty cell leaves the field unchanged if `replace` is `no`, and removes the field's value(s) if `replace` is `yes`.
    * To leave a field unchanged while `replace` is `yes`, put `!skip` in the cell. If the value `!skip` must literally be stored in a field, choose another
      skip code with the `--skip-code` option (default `!skip`).

### Command line defaults

The command line options `--expectedState`, `--expectInReview`, `--publishVersion`, `--versionNote` and `--replace` can be combined with `--input-file` and
provide default values for the corresponding columns, as described in [Default values](#default-values). If neither a column value nor a command line option
is given, no state expectation, publication or version note is applied.

[Enabling version notes]: https://guides.dataverse.org/en/latest/installation/config.html#dataverse-feature-enable-version-note

[Create a dataset]: {{ create_dataset_url }}

[Dataverse API]: {{ dataverse_api_url }}

[dans-dataverse-client-lib]: {{ dans_dataverse_client_lib_url }}

[Edit metadata]: {{ edit_metadata_url }}
