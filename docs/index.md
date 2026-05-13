dd-dv-cli
=========

Dataverse command-line interface

SYNOPSIS
--------

    dv [-hV] [COMMAND]
    dv banner-list
    dv dataset-publish

    dv --help # Show full list of commands


DESCRIPTION
-----------

Dataverse command-line interface.


EXAMPLES
--------

<!-- Add examples of invoking this module from the command line or via HTTP other interfaces -->
    

INSTALLATION AND CONFIGURATION
------------------------------
Currently, this project is built as an RPM package for RHEL8/Rocky8 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/dd-dv-cli` and the configuration files to `/etc/opt/dans.knaw.nl/dd-dv-cli`. 

BUILDING FROM SOURCE
--------------------
Prerequisites:

* Java 17 or higher
* Maven 3.3.3 or higher
* RPM

Steps:
    
    git clone https://github.com/DANS-KNAW/dd-dv-cli.git
    cd dd-dv-cli 
    mvn clean install
