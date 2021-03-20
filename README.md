<a href="https://www.mbartek.com">
<img src="https://www.mbartek.com/images/mbar-logo-bkd.png" align="right" alt="M-bar Technologies and Consulting, LLC" width="301" height="68">
</a>
# Weather Utilities for Synoptic API
-----------------------------------


**weather_utils** is a Python library providing local caching and utilities based on the [Synoptic API](https://developers.synopticdata.com/mesonet/).

Features that it includes are:


  * Local caching of weather station data
  * Search for peak gusts within specified distances and times
  * Random weather data for a given location within a specified time window


## Prerequisites

### Synoptic requirements
Users should familiarize themselves with the [Synoptic API](https://developers.synopticdata.com/mesonet/). 

Synoptic makes its API available for non-commercial use. Use of these utilities therefore be non-commercial unless alternative arrangement are made with Synoptic.

You will need to set up an account and obtain an API token in order to use the Synoptic API.

### System Requirements

  * Python 3
  * Linux (other OS TBD)
  * sqlite3 

## Installation

Currently runs as downloaded. Python module prerequisites include:

  * zulu
  * cPickle

## License

Gnu General Public License, version 3

## Examples

Examples can be found in the examples folder.
Currently, a PG&E ignition ignition analysis and paired Monte Carlo analysis are shown:
  * pge_ignitions_2015_2019.py
  * pge_ignitions_2015_2019_controlmc.py
These read data from an input page of an excel spreadsheet and create a new excel spreadsheet containing the processed data. 

## TBD

  * Input must be weather.ini. Needs to be an input parameter.
  * Creation of installer package





