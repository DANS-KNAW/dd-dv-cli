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
    * `datasetId` - the ID or PID of the dataset, a PID must contain the protocol, e.g., `doi:10.5072/FK2/123456`
* Preconditions
    * `expectedState` - the expected state of the dataset version to be edited, one of `draft`, `released`, `deacessioned` or a negation of one of those, e.g.,
      `not released`
    * `expectInReview` - whether the dataset is expected to be in review: `yes` or `no`
* Processing instructions
    * `publishVersion` - whether to publish the dataset after editing, one of `minor`, `major` or `leave-draft`.
    * `replace` - whether to replace existing values or add to them: `yes` or `no`
* Field values

### Field types

The names of the field value columns depend on the metadata blocks that are available.

* A primitive field or controlled vocabulary field is specified simply by the field identifier, e.g., `myField`.
* If multiple values are to be added, the field identifier is indexed, e.g., `myField[1]`. `myField[2]`, etc.
* A compound field is specified by the field identifier of the parent field, followed by a dot and the name of the child field, e.g., `myField.myChildField`.
  Note that in many cases the name of the child field refers to the parent field by convention, e.g., `authorName`, but you still have to prefix it with the
  parent field identifier, e.g., `author.authorName`.
* If multiple values of a compound field are to be added, the parent field identifier is indexed, e.g., `myField[1].myChildField`, `myField[2].myChildField`,
  etc.

### Running without a CSV input file

When testing, it can be useful to specify a row directly on the command-line, without going to the trouble of first saving it into a CSV file. The columns for
dataset identifier, preconditions and processing instructions all have their command line equivalent, e.g., `--datasetId`, `--expectedState`, etc. Field values
can be specified on the command line as trailing arguments in the format `<column name>=<value>`. Note that you will need to enclose the whole argument in
quotes if the value contains spaces, or if the name has an index, e.g., `'myField[1]=longer value with spaces'`.

### Report

When running with a CSV input file, a report will be written to a file in the directory configured in `editMetadata.reportsDir` in `.dv.yml`. The `--report-dir`
option can be used to override this location. The report contains a copy of each line with the extra columns `result` and `message`. When running without a CSV
input file, the report is written to the standard output.

[Dataverse API]: {{ dataverse_api_url }}
[dans-dataverse-client-lib]: {{ dans_dataverse_client_lib_url }}
[Edit metadata]: {{ edit_metadata_url }}