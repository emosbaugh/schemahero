package mysql

import (
	schemasv1alpha2 "github.com/schemahero/schemahero/pkg/apis/schemas/v1alpha2"
	"github.com/schemahero/schemahero/pkg/database/types"
)

func AlterColumnStatement(tableName string, primaryKeys []string, desiredColumns []*schemasv1alpha2.SQLTableColumn, existingColumn *types.Column) (string, error) {
	// this could be an alter or a drop column command
	for _, desiredColumn := range desiredColumns {
		if desiredColumn.Name == existingColumn.Name {
			column, err := schemaColumnToColumn(desiredColumn)
			if err != nil {
				return "", err
			}

			if columnsMatch(existingColumn, column) {
				return "", nil
			}

			isPrimaryKey := false
			for _, primaryKey := range primaryKeys {
				if column.Name == primaryKey {
					isPrimaryKey = true
				}
			}
			// primary key cannot be null
			if isPrimaryKey && column.Constraints != nil {
				column.Constraints.NotNull = nil
			}

			return AlterModifyColumnStatement{
				TableName: tableName,
				Column:    *column,
			}.String(), nil
		}
	}

	// wasn't found as a desired column, so drop
	return AlterDropColumnStatement{
		TableName: tableName,
		Column:    types.Column{Name: existingColumn.Name},
	}.String(), nil
}

func columnsMatch(col1 *types.Column, col2 *types.Column) bool {
	if col1.DataType != col2.DataType {
		return false
	}

	if col1.ColumnDefault != col2.ColumnDefault {
		return false
	}

	col1Constraints, col2Constraints := col1.Constraints, col2.Constraints
	if col1Constraints == nil {
		col1Constraints = &types.ColumnConstraints{}
	}
	if col2Constraints == nil {
		col2Constraints = &types.ColumnConstraints{}
	}

	return types.NotNullConstraintEquals(col1Constraints.NotNull, col2Constraints.NotNull)
}
