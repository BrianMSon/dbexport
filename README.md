# dbexport
- Export mysql data to CSV or SQL file.

# Usage
- Usage   : dbexport [SERVER1|SERVER2] DBName "sql query" outfilename
- Example : dbexport SERVER1 defaultdb "select * from tbl_test;" outfilename
- Example : dbexport -v SERVER1 defaultdb "select * from tbl_test;" outfilename
- Example : dbexport -sql SERVER1 defaultdb "select * from tbl_test;" outfilename
- Example : dbexport -sql -bulk SERVER1 defaultdb "select * from tbl_test;" outfilename
- Example : dbexport -debug SERVER1 defaultdb "select * from tbl_test;" outfilename

# Config file : dbexport.yml
- TargetDBs:
	- SERVER1:
		- username: USERNAME
		- password: PASSWORD
		- hostname: HOSTNAME
		- port: 3306
	- SERVER2:
		- username: USERNAME2
		- password: PASSWORD2
		- hostname: HOSTNAME2
		- port: 3306
