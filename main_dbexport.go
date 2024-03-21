package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/eiannone/keyboard"
	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Servers map[string]TargetDBInfo `yaml:"TargetDBs"`
}

type TargetDBInfo struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Hostname string `yaml:"hostname"`
	Port     string `yaml:"port"`
}

var configFileName = "dbexport.yml"

// Usage : dbexport [SERVER1|SERVER2] DBName "sql query" outfilename(without ext)
func printUsage() {
	println("Version : 1.0")
	println(`Usage   : dbexport [SERVER1|SERVER2] DBName "sql query" outfilename`)
	println(`Example : dbexport SERVER1 defaultdb "select * from tbl_test;" outfilename`)
	println(`Example : dbexport -v SERVER1 defaultdb "select * from tbl_test;" outfilename`)
	println(`Example : dbexport -sql SERVER1 defaultdb "select * from tbl_test;" outfilename`)
	println(`Example : dbexport -sql -bulk SERVER1 defaultdb "select * from tbl_test;" outfilename`)
	println(`Example : dbexport -debug SERVER1 defaultdb "select * from tbl_test;" outfilename`)
	println(`----------------------------`)
	println(`Config file : ` + configFileName)
	println(`----------------------------
TargetDBs:
  SERVER1:
    username: USERNAME
    password: PASSWORD
    hostname: HOSTNAME
    port: 3306
  SERVER2:
    username: USERNAME2
    password: PASSWORD2
    hostname: HOSTNAME2
    port: 3306
----------------------------`)
}

var (
	isVerbose = false
	isSqlOut  = false
	isDebug   = false
	isBulkOut = false
)

const maxLineSize = 102400 // 원하는 최대 줄 크기

func main() {
	targetDB, dbname, query, outfilename := parseArgs()

	if isDebug == true {
		println("- targetDB:", targetDB)
		println("- dbname:", dbname)
		println("- query:", query)
		println("- outfilename:", outfilename)
	}

	if query == "" {
		printUsage()
		pause()
		return
	}

	////////////////////////////////////////////////
	// query에서 tablename 추출
	tablename := getTableNameFromQuery(query)
	if isDebug == true {
		println("= tablename:", tablename)
	}
	outcsv_filename := outfilename + ".csv"
	outsql_filename := outfilename + ".sql"
	////////////////////////////////////////////////

	if isDebug == true {
		println("outcsv_filename : " + outcsv_filename)
		if isSqlOut == true {
			println("outsql_filename : " + outsql_filename)
		}
	}

	// Check if the query contains a SELECT statement.
	if strings.Contains(strings.ToLower(query), "select") == false {
		printError("SELECT statement must exist in the query.")
		return
	}

	// connectDB
	db, err := connectDB(targetDB, dbname)
	if err != nil {
		printError(err.Error())
		log.Fatal(err)
		return
	}
	defer db.Close()

	doQueryAndPrintResult(db, query, outcsv_filename, outsql_filename, tablename)
}

func parseArgs() (targetDB, dbname, query, outfilename string) {
	argOrder := 0
	for i, arg := range os.Args {
		if i < 1 {
			// skip filename
			continue
		}
		//println(arg)
		if arg[0] == '-' {
			if arg == "-v" {
				isVerbose = true
			} else if arg == "-debug" {
				isDebug = true
			} else if arg == "-sql" {
				isSqlOut = true
			} else if arg == "-bulk" {
				isBulkOut = true
			} else {
				// invalid option
				printError("Invalid option : " + arg)
				printUsage()
				os.Exit(1)
			}
		} else {
			argOrder++
			if argOrder == 1 {
				targetDB = arg
			} else if argOrder == 2 {
				dbname = arg
			} else if argOrder == 3 {
				query = arg
			} else if argOrder == 4 {
				outfilename = arg
			}
		}
	}

	return targetDB, dbname, query, outfilename
}

func loadConfigFromYML(targetDB string) (username, password, hostname, port string) {
	// Read YAML file
	yamlFile, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
		os.Exit(1)
	}

	var config Config

	// Parsing YAML
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
		os.Exit(1)
	}

	if serverInfo, ok := config.Servers[targetDB]; ok {
		// fmt.Printf("%s Info:\n", targetDB)
		// fmt.Println("Username:", serverInfo.Username)
		// fmt.Println("Password:", serverInfo.Password)
		// fmt.Println("Hostname:", serverInfo.Hostname)
		// fmt.Println("Port:", serverInfo.Port)

		username = serverInfo.Username
		password = serverInfo.Password
		hostname = serverInfo.Hostname
		port = serverInfo.Port
	} else {
		printError("Not found in configuration : " + targetDB)
		os.Exit(1)
	}

	return username, password, hostname, port
}

func connectDB(targetDB, dbname string) (*sql.DB, error) {
	username, password, hostname, port := loadConfigFromYML(targetDB)

	return sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, hostname, port, dbname))
}

func getTableNameFromQuery(query string) string {
	tablename := ""
	temp := strings.ToLower(query)
	idx := strings.Index(temp, "from")
	if idx >= 0 {
		temp = strings.Trim(temp[idx+len("from"):], " ")
		endidx := strings.Index(temp, " ")
		if endidx >= 0 {
			temp = strings.Trim(temp[:endidx], " ")
			tablename += temp
		} else {
			endidx = strings.Index(temp, ";")
			if endidx >= 0 {
				temp = strings.Trim(temp[:endidx], " ")
				tablename += temp
			}
		}
	}
	return tablename
}

func doQueryAndPrintResult(db *sql.DB, query, outcsv_filename, outsql_filename, tablename string) {
	// comma separator
	separator := ","

	rows, err := db.Query(query)
	if err != nil {
		printError(err.Error())
		log.Fatal(err)
	}
	defer rows.Close()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		printError(err.Error())
		log.Fatal(err)
	}

	// Create a slice to hold the values for each row
	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(columns))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	csvHeader := strings.Join(columns, separator)

	// Print the column names
	if isVerbose == true {
		println(csvHeader)
	}

	var recordList []string

	// Iterate over the rows and print the values
	for rows.Next() {
		err := rows.Scan(valuePointers...)
		if err != nil {
			printError(err.Error())
			log.Fatal(err)
		}

		var rowValues []string
		for _, v := range values {
			if v == nil {
				rowValues = append(rowValues, "NULL")
			} else {
				// Convert byte arrays to strings
				if b, ok := v.([]byte); ok {
					data := fmt.Sprintf(`%s`, string(b))
					data = strings.ReplaceAll(data, "\n", "\\n")
					if strings.Contains(data, ",") == true {
						data = "\"" + data + "\""
					}
					rowValues = append(rowValues, data)
				} else {
					data := fmt.Sprintf(`%v`, v)
					data = strings.ReplaceAll(data, "\n", "\\n")
					if strings.Contains(data, ",") == true {
						data = "\"" + data + "\""
					}
					rowValues = append(rowValues, data)
				}
			}
		}

		record := strings.Join(rowValues, separator)

		// Print record
		if isVerbose == true {
			println(record)
		}

		// add to recordList
		recordList = append(recordList, record)
	}

	if err := rows.Err(); err != nil {
		printError(err.Error())
		log.Fatal(err)
		return
	}

	// Save to CSV file
	saveToCSVFile(outcsv_filename, csvHeader, recordList)

	if isSqlOut == true {
		// Save to SQL file (insert query)
		saveToSQLFile(outcsv_filename, outsql_filename, csvHeader, tablename, recordList)

		if isBulkOut == true {
			saveBulkInsertQuery(outsql_filename)
		}
	}
}

func saveToCSVFile(csvfilename, csvHeader string, recordList []string) {
	outfile, err := os.Create(csvfilename)
	if err != nil {
		log.Println(err)
		return
	}
	defer outfile.Close()

	outfile.WriteString(csvHeader + "\n")

	for _, line := range recordList {
		outfile.WriteString(line + "\n")
	}
}

func saveToSQLFile(csvfilename, outsql_filename, csvHeader, tablename string, recordList []string) {
	// Open CSV file
	file, err := os.Open(csvfilename)
	if err != nil {
		printError(err.Error())
		log.Fatal(err)
		return
	}

	// Start to read CSV file
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	// Read data from CSV file
	rowCount := 0
	var data [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			printError(err.Error())
			log.Fatal(err)
		}
		rowCount++
		//println(record) // print record
		if rowCount == 1 {
			continue
		}
		data = append(data, record)
	}

	var queryLines []string

	queryCount := 0
	for _, rows := range data {
		strQuery := "INSERT INTO " + tablename + " (" + csvHeader + ") VALUES ("
		//println(rows)

		for i, row := range rows {
			row = strings.Trim(row, " ")
			if i > 0 {
				strQuery += ","
			}
			// 데이터에 ' 가 있으면 Escape 처리
			row = strings.ReplaceAll(row, "'", "\\'")
			if strings.ToUpper(row) == "NULL" || strings.ToUpper(row) == "(NULL)" {
				// NULL 처리 (작은 따옴표로 감싸지 않음)
				strQuery += "NULL"
			} else {
				// 작은 따옴표로 감싸기
				strQuery += "'"
				strQuery += row
				strQuery += "'"
			}
		}
		strQuery += ");"

		queryLines = append(queryLines, strQuery)

		queryCount++
	}

	file.Close()

	// Save to SQL file
	outfile, err := os.Create(outsql_filename)
	if err != nil {
		log.Println(err)
		return
	}
	defer outfile.Close()

	for _, line := range queryLines {
		outfile.WriteString(line + "\n")
	}
}

func printError(errMsg string) {
	color.HiRed("---------!!!!!!!!!!!!!!!!!!!!!----------")
	color.HiRed("---------ERROR!!!!!!!!!!!!!!!!---------- : " + errMsg)
	color.HiRed("---------!!!!!!!!!!!!!!!!!!!!!----------")
}

func pause() {
	println("\nPress Any key...")
	_, _, err := keyboard.GetSingleKey()
	if err != nil {
		println("Error:", err)
		os.Exit(1)
	}
}

//-------------------------------------------------

func saveBulkInsertQuery(filename string) {
	isReplaceOrgFile := false
	isSilent := true
	outfilename := filename + ".bulk"

	convertInsertQuery_To_BulkInsertQuery(filename, outfilename, isReplaceOrgFile, isSilent)

	// verify each lines
	verifyEachFileLines(filename, outfilename)
}

func convertInsertQuery_To_BulkInsertQuery(filename, outfilename string, isReplaceOrgFile, isSilent bool) {
	// SQL Text 파일 열기
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	var bulklines []string

	///////////////////////////////////////////
	// 라인별 읽기
	insertStatement := ""
	lineCount := 0
	bytesCount := 0
	scanner := bufio.NewScanner(file)
	//////////////////////////
	// 라인 최대 버퍼 조정
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, maxLineSize)
	//////////////////////////
	for scanner.Scan() {
		lineCount++
		line := scanner.Text()
		//fmt.Println(lineCount, " :: ", line)

		convertedLine := line

		bytesCount += len(line)

		//fmt.Println("bytesCount:", bytesCount)

		isInsertStatmentAdded := false
		// 1000 라인씩 끊어서 쿼리 작성 (4MB 넘어가지 않도록)
		if insertStatement != "" && (lineCount%1000 == 0 || bytesCount >= 3900000) {
			lineCount = 0
			bytesCount = 0
			///////////////////////////////////////////
			// 마지막줄 ), 는 ); 로 변경
			lastLine := bulklines[len(bulklines)-1]
			if len(lastLine) > 3 {
				idxEndParentheses := strings.LastIndex(lastLine, "),")
				if idxEndParentheses >= len(lastLine)-3 {
					// 마지막 , 를 빼고 ; 로 대체
					lastLine = lastLine[:len(lastLine)-1] + ";"
					// 기존 마지막줄 제거하고
					bulklines = bulklines[:len(bulklines)-1]
					// 변경한 lastLine을 추가하자
					bulklines = append(bulklines, lastLine)
				}
			}
			///////////////////////////////////////////

			//bulklines = append(bulklines, "-- --------------------------------")
			// INSERT INTO문 추가
			bulklines = append(bulklines, insertStatement)
			//fmt.Println("DEBUG : insertStatement appended. (" + insertStatement + ")")
			isInsertStatmentAdded = true
		}

		// INSERT문 추출 (INSERT INTO ~ VALUES)
		startidx := strings.Index(line, "INSERT INTO")
		if startidx >= 0 {
			if insertStatement == "" {
				endidx := strings.Index(line, "VALUES")
				if endidx >= 0 {
					insertStatement = line[startidx : endidx+len("VALUES")]
					if isSilent == false {
						fmt.Println("-- insertStatement: " + insertStatement)
					}
					// INSERT INTO문 추가 (첫줄)
					bulklines = append(bulklines, insertStatement)
				}
			}

			// INSERT INTO .* VALUES 를 첫 행에만 추가하고 나머지는 제거
			// ); 를 ) 로 변환하고 ,를 붙이자
			convertedLine = convertInsertIntoLine(line, filename)
			//fmt.Println("==> " + convertedLine)
		} else {
			// INSERT문이 아니면 그대로 넣어준다.
			convertedLine = line

			// INSERT문 초기화
			if insertStatement != "" {
				insertStatement = ""
				//fmt.Println("---end---")
				lineCount = 0

				///////////////////////////////////////////
				// 마지막줄 ), 는 ); 로 변경
				lastLine := bulklines[len(bulklines)-1]
				if len(lastLine) > 3 {
					idxEndParentheses := strings.LastIndex(lastLine, "),")
					if idxEndParentheses >= len(lastLine)-3 {
						// 마지막 , 를 빼고 ; 로 대체
						lastLine = lastLine[:len(lastLine)-1] + ";"
						// 기존 마지막줄 제거하고
						bulklines = bulklines[:len(bulklines)-1]
						// 변경한 lastLine을 추가하자
						bulklines = append(bulklines, lastLine)
					}
				}
				///////////////////////////////////////////

				// 한 테이블이 끝날 때 마지막줄이 INSERT INTO 이면 지워준다. 2023-11-02
				if isInsertStatmentAdded == true && strings.Index(lastLine, "INSERT INTO") == 0 {
					//fmt.Println("DEBUG : " + lastLine)
					// 기존 마지막줄 제거하고
					bulklines = bulklines[:len(bulklines)-1]
				}
			}
		}

		bulklines = append(bulklines, convertedLine)
	}

	///////////////////////////////////////////
	// VALUES의 마지막줄 ), 는 ); 로 변경
	lastLine := bulklines[len(bulklines)-1]
	//fmt.Println("===============> lastLine : " + lastLine)
	idxValues := strings.Index(lastLine, "VALUES")
	//fmt.Println("===============> idxValues : ", idxValues)
	if idxValues > 0 {
		//fmt.Println("===============> len(lastLine) : ", len(lastLine))
		// 마지막 , 를 빼고 ; 로 대체
		lastLine = lastLine[:len(lastLine)-1] + ";"
		// 기존 마지막줄 제거하고
		bulklines = bulklines[:len(bulklines)-1]
		// 변경한 lastLine을 추가하자
		bulklines = append(bulklines, lastLine)
	} else {
		///////////////////////////////////////////
		// 마지막줄 ), 는 ); 로 변경
		lastLine := bulklines[len(bulklines)-1]
		if len(lastLine) > 3 {
			idxEndParentheses := strings.LastIndex(lastLine, "),")
			if idxEndParentheses >= len(lastLine)-3 {
				// 마지막 , 를 빼고 ; 로 대체
				lastLine = lastLine[:len(lastLine)-1] + ";"
				// 기존 마지막줄 제거하고
				bulklines = bulklines[:len(bulklines)-1]
				// 변경한 lastLine을 추가하자
				bulklines = append(bulklines, lastLine)
			}
		}
		///////////////////////////////////////////
	}
	///////////////////////////////////////////

	// 입력 파일 닫기
	file.Close()

	if isReplaceOrgFile == true {
		outfilename = filename
	}

	// 파일 출력
	outfile, err := os.Create(outfilename)
	if err != nil {
		printError("Create outfile error! (" + outfilename + ")")
		log.Fatal("<2>", err)
	}

	// 각 라인마다 파일에 쓰기
	for _, line := range bulklines {
		fmt.Fprintln(outfile, line)
		if isSilent == false {
			fmt.Println(line) // debug
		}
	}

	// 출력 파일 닫기
	outfile.Close()
	//fmt.Println("-----------------------------------------------")
}

func convertInsertIntoLine(line string, filename string) string {
	// INSERT INTO .* VALUES 제거하고 끝에 있는 ; 제거하고 , 를 붙이자
	retline := ""
	//fmt.Println("line : " + line)

	startidx := strings.Index(line, "INSERT INTO")
	if startidx >= 0 {
		endidx := strings.Index(line, "VALUES")
		if endidx >= 0 {
			retline = line[endidx+len("VALUES"):]

			lastidx := strings.LastIndex(retline, ");")
			if lastidx >= 0 {
				retline = retline[:lastidx+1] + ","
			}
		}
	}

	//fmt.Println("retline : " + retline)
	return retline
}

func countLinesInFile(filename string, excludeString string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	lineCount := 0
	scanner := bufio.NewScanner(file)
	//////////////////////////
	// 라인 최대 버퍼 조정
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, maxLineSize)
	//////////////////////////
	for scanner.Scan() {
		line := scanner.Text()

		// "INSERT INTO"로 시작하는 라인을 제외
		if excludeString != "" && strings.HasPrefix(line, excludeString) {
			continue
		}

		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}

func verifyEachFileLines(filename, filename2 string) {
	lines1, err1 := countLinesInFile(filename, "")
	if err1 != nil {
		printError("countLinesInFile(filename) error!")
		return
	}

	lines2, err2 := countLinesInFile(filename2, "INSERT INTO ")
	if err2 != nil {
		printError("countLinesInFile(filename2) error!")
		return
	}

	if lines1 != lines2 {
		printError("BulkSQL verification lines failed...")
	} else {
		color.Green("BulkSQL verification Ok.")
	}
}
