package DatabaseControl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/HazelnutParadise/Go-Utils/jsonutil"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
)

var db map[string]*sql.DB

func InitAndSetRoutes(router *gin.RouterGroup) {
	databasePaths, _ := jsonutil.LoadJSONFileAndExtractSubMap("config.json", "databases")
	db = make(map[string]*sql.DB)

	for key, path := range databasePaths {
		db[key] = initDB(path.(string))
	}

	router.POST("/tables", createTable)
	router.Any("/record", manageRecords)
	router.POST("/trigger", createTrigger)
	router.POST("/sql", executeSQL)
}

func initDB(databasePath string) *sql.DB {
	database, err := sql.Open("sqlite3", databasePath)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return database
}

func createTable(c *gin.Context) {
	var req struct {
		Tables   map[string][]map[string]string `json:"tables"`
		Database string                         `json:"database"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid JSON"})
		return
	}

	database := req.Database
	if database == "" {
		database = "main"
	}

	for tableName, fields := range req.Tables {
		attributes := fields[0]
		foreignKeys := fields[1]

		sqlStmt := buildCreateTableSQL(tableName, attributes, foreignKeys)
		if _, err := db[database].Exec(sqlStmt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func manageRecords(c *gin.Context) {
	switch c.Request.Method {
	case http.MethodPost:
		insertRecord(c)
	case http.MethodDelete:
		deleteRecord(c)
	case http.MethodGet:
		queryRecords(c)
	case http.MethodPut:
		updateRecord(c)
	default:
		c.JSON(http.StatusMethodNotAllowed, gin.H{"status": "error", "message": "Method Not Allowed"})
	}
}

func insertRecord(c *gin.Context) {
	var req struct {
		Relation string                 `json:"relation"`
		Records  map[string]interface{} `json:"records"`
		Database string                 `json:"database"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid JSON"})
		return
	}

	database := req.Database
	if database == "" {
		database = "main"
	}

	sqlStmt, values := buildInsertSQL(req.Relation, req.Records)
	if _, err := db[database].Exec(sqlStmt, values...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func deleteRecord(c *gin.Context) {
	relation := c.Query("relation")
	conditionsStr := c.Query("conditions")
	database := c.Query("database")
	if database == "" {
		database = "main"
	}

	var conditions map[string]interface{}
	if err := json.Unmarshal([]byte(conditionsStr), &conditions); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON for conditions"})
		return
	}

	sqlStmt, values := buildDeleteSQL(relation, conditions)
	if _, err := db[database].Exec(sqlStmt, values...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func queryRecords(c *gin.Context) {
	relation := c.Query("relation")
	conditionsStr := c.Query("conditions")
	database := c.Query("database")
	if database == "" {
		database = "main"
	}

	var conditions map[string]interface{}
	if conditionsStr != "" {
		if err := json.Unmarshal([]byte(conditionsStr), &conditions); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON for conditions"})
			return
		}
	}

	toQuery := c.QueryArray("to_query")
	returnAsDict := c.Query("return_as_dict") == "true"

	sqlStmt, values := buildSelectSQL(relation, conditions, toQuery)
	rows, err := db[database].Query(sqlStmt, values...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	defer rows.Close()

	var results interface{}
	if returnAsDict {
		results, err = rowsToDict(rows)
	} else {
		results, err = rowsToList(rows)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "result": results})
}

func updateRecord(c *gin.Context) {
	var req struct {
		Relation   string                 `json:"relation"`
		Conditions map[string]interface{} `json:"conditions"`
		NewValues  map[string]interface{} `json:"new_values"`
		Database   string                 `json:"database"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid JSON"})
		return
	}

	database := req.Database
	if database == "" {
		database = "main"
	}

	sqlStmt, values := buildUpdateSQL(req.Relation, req.Conditions, req.NewValues)
	if _, err := db[database].Exec(sqlStmt, values...); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func createTrigger(c *gin.Context) {
	var req struct {
		TriggerName     string `json:"trigger_name"`
		Action          string `json:"action"`
		TableName       string `json:"table_name"`
		TriggeringEvent string `json:"triggering_event"`
		SQLOperation    string `json:"sql_operation"`
		Database        string `json:"database"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid JSON"})
		return
	}

	database := req.Database
	if database == "" {
		database = "main"
	}

	sqlStmt := buildCreateTriggerSQL(req.TriggerName, req.Action, req.TableName, req.TriggeringEvent, req.SQLOperation)
	if _, err := db[database].Exec(sqlStmt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func executeSQL(c *gin.Context) {
	var req struct {
		SQLStatement     string        `json:"sql_statement"`
		Database         string        `json:"database"`
		PlaceholdersMode bool          `json:"placeholders_mode"`
		ValuesTuple      []interface{} `json:"values_tuple"`
		QueryMode        bool          `json:"query_mode"`
		FKMode           bool          `json:"fk_mode"`
		ReturnAsDict     bool          `json:"return_as_dict"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid JSON"})
		return
	}

	database := req.Database
	if database == "" {
		database = "main"
	}

	if req.FKMode {
		db[database].Exec("PRAGMA foreign_keys = ON;")
	}
	db[database].Exec("PRAGMA journal_mode = WAL;")

	if req.QueryMode {
		rows, err := db[database].Query(req.SQLStatement, req.ValuesTuple...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
		defer rows.Close()

		var results interface{}
		if req.ReturnAsDict {
			results, err = rowsToDict(rows)
		} else {
			results, err = rowsToList(rows)
		}

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "success", "result": results})
	} else {
		_, err := db[database].Exec(req.SQLStatement, req.ValuesTuple...)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	}
}

// SQL 構建函數
func buildCreateTableSQL(relation string, attributes, foreignKeys map[string]string) string {
	columns := []string{}
	for name, dataType := range attributes {
		columns = append(columns, fmt.Sprintf("%s %s", name, dataType))
	}

	for fkColumn, references := range foreignKeys {
		columns = append(columns, fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s ON UPDATE CASCADE ON DELETE CASCADE", fkColumn, references))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", relation, strings.Join(columns, ", "))
}

func buildInsertSQL(relation string, records map[string]interface{}) (string, []interface{}) {
	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}
	for column, value := range records {
		columns = append(columns, column)
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", relation, strings.Join(columns, ", "), strings.Join(placeholders, ", ")), values
}

func buildDeleteSQL(relation string, conditions map[string]interface{}) (string, []interface{}) {
	whereClause := []string{}
	values := []interface{}{}
	for column, value := range conditions {
		whereClause = append(whereClause, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", relation, strings.Join(whereClause, " AND ")), values
}

func buildSelectSQL(relation string, conditions map[string]interface{}, toQuery []string) (string, []interface{}) {
	columns := "*"
	if len(toQuery) > 0 {
		columns = strings.Join(toQuery, ", ")
	}
	whereClause := []string{}
	values := []interface{}{}
	for column, value := range conditions {
		whereClause = append(whereClause, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", columns, relation)
	if len(whereClause) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(whereClause, " AND "))
	}
	return query + ";", values
}

func buildUpdateSQL(relation string, conditions, newValues map[string]interface{}) (string, []interface{}) {
	setClause := []string{}
	whereClause := []string{}
	values := []interface{}{}
	for column, value := range newValues {
		setClause = append(setClause, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}
	for column, value := range conditions {
		whereClause = append(whereClause, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", relation, strings.Join(setClause, ", "), strings.Join(whereClause, " AND ")), values
}

func buildCreateTriggerSQL(triggerName, action, tableName, triggeringEvent, sqlOperation string) string {
	return fmt.Sprintf(`
	CREATE TRIGGER IF NOT EXISTS %s
	%s ON %s
	FOR EACH ROW
	WHEN (%s)
	BEGIN
		%s;
	END;`, triggerName, action, tableName, triggeringEvent, sqlOperation)
}

// 工具函數
func rowsToDict(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}
	for rows.Next() {
		row := make(map[string]interface{})
		columnPointers := make([]interface{}, len(columns))
		for i := range columns {
			columnPointers[i] = new(interface{})
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		for i, colName := range columns {
			row[colName] = *(columnPointers[i].(*interface{}))
		}
		results = append(results, row)
	}
	return results, nil
}

func rowsToList(rows *sql.Rows) ([][]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := [][]interface{}{}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnPointers {
			columnPointers[i] = new(interface{})
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		for i := range columns {
			row[i] = *(columnPointers[i].(*interface{}))
		}
		results = append(results, row)
	}
	return results, nil
}
