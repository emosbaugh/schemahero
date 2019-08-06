package generate

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	schemasv1alpha2 "github.com/schemahero/schemahero/pkg/apis/schemas/v1alpha2"
	"github.com/schemahero/schemahero/pkg/database/interfaces"
	"github.com/schemahero/schemahero/pkg/database/mysql"
	"github.com/schemahero/schemahero/pkg/database/postgres"
	"github.com/schemahero/schemahero/pkg/database/types"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Generator struct {
	Viper *viper.Viper
}

func NewGenerator() *Generator {
	return &Generator{
		Viper: viper.GetViper(),
	}
}

func (g *Generator) RunSync() error {
	fmt.Printf("connecting to %s\n", g.Viper.GetString("uri"))

	var db interfaces.SchemaHeroDatabaseConnection
	if g.Viper.GetString("driver") == "postgres" {
		pgDb, err := postgres.Connect(g.Viper.GetString("uri"))
		if err != nil {
			return err
		}
		db = pgDb
	} else if g.Viper.GetString("driver") == "mysql" {
		mysqlDb, err := mysql.Connect(g.Viper.GetString("uri"))
		if err != nil {
			return err
		}
		db = mysqlDb
	}

	tableNames, err := db.ListTables()
	if err != nil {
		fmt.Printf("%#v\n", err)
		return err
	}

	filesWritten := make([]string, 0, 0)
	for _, tableName := range tableNames {
		primaryKey, err := db.GetTablePrimaryKey(tableName)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return err
		}

		foreignKeys, err := db.ListTableForeignKeys(g.Viper.GetString("dbname"), tableName)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return err
		}

		indexes, err := db.ListTableIndexes(g.Viper.GetString("dbname"), tableName)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return err
		}

		columns, err := db.GetTableSchema(tableName)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return err
		}

		tableYAML, err := generateTableYAML(g.Viper.GetString("driver"), g.Viper.GetString("dbname"), tableName, primaryKey, foreignKeys, indexes, columns)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return err
		}

		// If there was a outputdir set, write it, else print it
		if g.Viper.GetString("output-dir") != "" {
			if err := ioutil.WriteFile(filepath.Join(g.Viper.GetString("output-dir"), fmt.Sprintf("%s.yaml", sanitizeName(tableName))), []byte(tableYAML), 0644); err != nil {
				return err
			}

			filesWritten = append(filesWritten, fmt.Sprintf("./%s.yaml", sanitizeName(tableName)))
		} else {

			fmt.Println(tableYAML)
			fmt.Println("---")
		}
	}

	// If there was an output-dir, write a kustomization.yaml too -- this should be optional
	if g.Viper.GetString("output-dir") != "" {
		kustomization := struct {
			Resources []string `yaml:"resources"`
		}{
			filesWritten,
		}

		kustomizeDoc, err := yaml.Marshal(kustomization)
		if err != nil {
			return err
		}

		if err := ioutil.WriteFile(filepath.Join(g.Viper.GetString("output-dir"), "kustomization.yaml"), kustomizeDoc, 0644); err != nil {
			return err
		}
	}
	return nil
}

func generateTableYAML(driver string, dbName string, tableName string, primaryKey *types.KeyConstraint, foreignKeys []*types.ForeignKey, indexes []*types.Index, columns []*types.Column) (string, error) {
	schemaForeignKeys := make([]*schemasv1alpha2.SQLTableForeignKey, 0, 0)
	for _, foreignKey := range foreignKeys {
		schemaForeignKey := types.ForeignKeyToSchemaForeignKey(foreignKey)
		schemaForeignKeys = append(schemaForeignKeys, schemaForeignKey)
	}

	schemaIndexes := make([]*schemasv1alpha2.SQLTableIndex, 0, 0)
	for _, index := range indexes {
		schemaIndex := types.IndexToSchemaIndex(index)
		schemaIndexes = append(schemaIndexes, schemaIndex)
	}

	schemaTableColumns := make([]*schemasv1alpha2.SQLTableColumn, 0, 0)
	for _, column := range columns {
		schemaTableColumn, err := types.ColumnToSchemaColumn(column)
		if err != nil {
			fmt.Printf("%#v\n", err)
			return "", err
		}

		schemaTableColumns = append(schemaTableColumns, schemaTableColumn)
	}

	tableSchema := &schemasv1alpha2.SQLTableSchema{
		PrimaryKey:  primaryKey.Columns,
		Columns:     schemaTableColumns,
		ForeignKeys: schemaForeignKeys,
		Indexes:     schemaIndexes,
	}

	schema := &schemasv1alpha2.TableSchema{}

	if driver == "postgres" {
		schema.Postgres = tableSchema
	} else if driver == "mysql" {
		schema.Mysql = tableSchema
	}

	schemaHeroResource := schemasv1alpha2.TableSpec{
		Database: dbName,
		Name:     tableName,
		Requires: []string{},
		Schema:   schema,
	}

	specDoc := struct {
		Spec schemasv1alpha2.TableSpec `yaml:"spec"`
	}{
		schemaHeroResource,
	}

	b, err := yaml.Marshal(&specDoc)
	if err != nil {
		fmt.Printf("%#v\n", err)
		return "", err
	}

	// TODO consider marshaling this instead of inline
	tableDoc := fmt.Sprintf(`apiVersion: schemas.schemahero.io/v1alpha2
kind: Table
metadata:
  name: %s
%s`, sanitizeName(tableName), b)

	return tableDoc, nil

}

func sanitizeName(name string) string {
	return strings.Replace(name, "_", "-", -1)
}
