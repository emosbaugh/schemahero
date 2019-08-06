package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/schemahero/schemahero/pkg/database/types"
)

func (m *MysqlConnection) ListTables() ([]string, error) {
	query := "select table_name from information_schema.TABLES where TABLE_SCHEMA = ?"

	rows, err := m.db.Query(query, m.databaseName)
	if err != nil {
		return nil, err
	}

	tableNames := make([]string, 0, 0)
	for rows.Next() {
		tableName := ""
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		tableNames = append(tableNames, tableName)
	}

	return tableNames, nil
}

func (m *MysqlConnection) ListTableIndexes(databaseName string, tableName string) ([]*types.Index, error) {
	query := `select
	index_name,
	non_unique,
	group_concat(column_name order by seq_in_index)
 	from information_schema.statistics
	 where table_name = ?
	 and index_name != 'PRIMARY'
	 and index_name not in (
	  select kcu.CONSTRAINT_NAME
	  from information_schema.KEY_COLUMN_USAGE kcu
	  inner join information_schema.TABLE_CONSTRAINTS tc
	    on tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	  inner join information_schema.REFERENTIAL_CONSTRAINTS rc
	    on rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	  where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
	  and kcu.TABLE_NAME = ?
        )
	group by 1, 2`
	rows, err := m.db.Query(query, tableName, tableName)
	if err != nil {
		return nil, err
	}

	indexes := make([]*types.Index, 0, 0)
	for rows.Next() {
		var index types.Index
		var columns string
		var nonUnique bool
		if err := rows.Scan(&index.Name, &nonUnique, &columns); err != nil {
			return nil, err
		}

		index.IsUnique = !nonUnique

		// columns are selected as col1,col2
		columnNames := strings.Split(columns, ",")
		index.Columns = columnNames

		indexes = append(indexes, &index)
	}

	return indexes, nil
}

func (m *MysqlConnection) ListTableForeignKeys(databaseName string, tableName string) ([]*types.ForeignKey, error) {
	query := `select
	kcu.COLUMN_NAME, kcu.CONSTRAINT_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE
	from information_schema.KEY_COLUMN_USAGE kcu
	inner join information_schema.TABLE_CONSTRAINTS tc
  	  on tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	inner join information_schema.REFERENTIAL_CONSTRAINTS rc
	  on rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
	and kcu.TABLE_NAME = ?
	and kcu.CONSTRAINT_SCHEMA = ?`

	rows, err := m.db.Query(query, tableName, databaseName)
	if err != nil {
		return nil, err
	}

	foreignKeys := make([]*types.ForeignKey, 0, 0)
	for rows.Next() {
		var childColumn, parentColumn, parentTable, name, deleteRule string

		if err := rows.Scan(&childColumn, &name, &parentTable, &parentColumn, &deleteRule); err != nil {
			return nil, err
		}

		foreignKey := types.ForeignKey{
			Name:          name,
			ParentTable:   parentTable,
			OnDelete:      deleteRule,
			ChildColumns:  []string{childColumn},
			ParentColumns: []string{parentColumn},
		}

		for _, foundFk := range foreignKeys {
			if foundFk.Name == name {
				foundFk.ChildColumns = append(foreignKey.ChildColumns, childColumn)
				foundFk.ParentColumns = append(foreignKey.ParentColumns, parentColumn)

				goto Appended
			}
		}

		foreignKeys = append(foreignKeys, &foreignKey)

	Appended:
	}

	return foreignKeys, nil
}

func (m *MysqlConnection) GetTablePrimaryKey(tableName string) (*types.KeyConstraint, error) {
	query := `select tc.CONSTRAINT_NAME, distinct(c.COLUMN_NAME)
from information_schema.TABLE_CONSTRAINTS tc
join information_schema.KEY_COLUMN_USAGE as kcu using (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
join information_schema.COLUMNS as c on c.TABLE_SCHEMA = tc.CONSTRAINT_SCHEMA
  and tc.TABLE_NAME = c.TABLE_NAME
  and kcu.TABLE_NAME = c.TABLE_NAME
  and kcu.COLUMN_NAME = c.COLUMN_NAME
where tc.CONSTRAINT_TYPE = 'PRIMARY KEY' and tc.TABLE_NAME = ?
order by c.ORDINAL_POSITION`

	rows, err := m.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}

	key := types.KeyConstraint{
		IsPrimary: true,
	}
	for rows.Next() {
		var constraintName, columnName string

		if err := rows.Scan(&constraintName, &columnName); err != nil {
			return nil, err
		}

		key.Name = constraintName
		key.Columns = append(key.Columns, columnName)
	}

	return &key, nil
}

func (m *MysqlConnection) GetTableSchema(tableName string) ([]*types.Column, error) {
	query := `select COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH from information_schema.COLUMNS where TABLE_NAME = ? order by ORDINAL_POSITION`
	rows, err := m.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}

	columns := make([]*types.Column, 0, 0)

	for rows.Next() {
		column := types.Column{}

		var maxLength sql.NullInt64
		var isNullable string
		var columnDefault sql.NullString

		if err := rows.Scan(&column.Name, &columnDefault, &isNullable, &column.DataType, &maxLength); err != nil {
			return nil, err
		}

		if isNullable == "NO" {
			column.Constraints = &types.ColumnConstraints{
				NotNull: &trueValue,
			}
		} else {
			column.Constraints = &types.ColumnConstraints{
				NotNull: &falseValue,
			}
		}

		if columnDefault.Valid {
			column.ColumnDefault = &columnDefault.String
		}

		if maxLength.Valid {
			column.DataType = fmt.Sprintf("%s (%d)", column.DataType, maxLength.Int64)
		}
		columns = append(columns, &column)
	}

	return columns, nil
}
