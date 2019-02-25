# Routes_calculation
Calculate routes (distance, travel time) between geographic points using API.

<ul>
<li>db_connect.py - connects to SQLite database;</li>
<li>geoYN.py - contains functions to work with API and main work cycle;</li>
<li>routes_calc.py - starting module for initialization.</li>
</ul>
<hr>

### db_connect_sql.py

Module to work with database.

class `DBConnect`() - establishes connection to database.

Uses methods:

`count_empty_rows`() - returns number of point pairs that have to be updated.

`raw_query`(<i>query</i>) - executes <i>query</i> and returns result.

`empty_dist`() - returns info about the next pair of points.

`update_dist`(<i>id_, km_time</i>) - uploads to server info contained in <i>km_time</i>. Updates row with corresponding <i>id_</i>.

### geoYN.py

Module to work with API and transfer data between functions.

Uses functions:

`not_valid_response`(<i>text</i>) - checks <i>text</i> of responce for errors.

`get_km_time`(<i>n, latA, lonA, latB, lonB, id_</i>) - asks API for distance and time between point with coordinates (<i>latA, lonA</i>) and point with coordinates (<i>latB, lonB</i>). <i>n</i> - parameter for API, 
 <i>id_</i> - current row id for log error.

`geoYN`(<i>args, db_params</i>) - main cycle that requests server, API and transfers data between them.

### routes_calc.py

Main module that makes checks and initialize parameters.

<hr>

Usage: `routes_calc.py` with parameters. Use `--help` for additional information. 

Be sure to setup `config.ini`.

<hr>

## Requirements

* [pyodbc](https://github.com/mkleehammer/pyodbc)

<hr>

### Update history

*  1.02 - Added `timeout` parameter to handle situation if server has not issued a response.
*  1.01 - Release version.