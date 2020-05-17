package database

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	schemasv1alpha3 "github.com/schemahero/schemahero/pkg/apis/schemas/v1alpha3"
	"github.com/schemahero/schemahero/pkg/database/mysql"
	"github.com/schemahero/schemahero/pkg/database/postgres"
	"github.com/schemahero/schemahero/pkg/logger"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type Database struct {
	Viper *viper.Viper
}

func NewDatabase() *Database {
	return &Database{
		Viper: viper.GetViper(),
	}
}

func (d *Database) CreateFixturesSync() error {
	logger.Infof("generating fixtures",
		zap.String("input-dir", d.Viper.GetString("input-dir")))

	statements := []string{}
	handleFile := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		fileData, err := ioutil.ReadFile(filepath.Join(d.Viper.GetString("input-dir"), info.Name()))
		if err != nil {
			return err
		}

		var spec *schemasv1alpha3.TableSpec

		parsedK8sObject := schemasv1alpha3.Table{}
		if err := yaml.Unmarshal(fileData, &parsedK8sObject); err == nil {
			if parsedK8sObject.Spec.Schema != nil {
				spec = &parsedK8sObject.Spec
			}
		}

		if spec == nil {
			plainSpec := schemasv1alpha3.TableSpec{}
			if err := yaml.Unmarshal(fileData, &plainSpec); err != nil {
				return err
			}

			spec = &plainSpec
		}

		if spec.Schema == nil {
			return nil
		}

		if d.Viper.GetString("driver") == "postgres" {
			if spec.Schema.Postgres == nil {
				return nil
			}

			statement, err := postgres.CreateTableStatement(spec.Name, spec.Schema.Postgres)
			if err != nil {
				return err
			}

			statements = append(statements, statement)
		} else if d.Viper.GetString("driver") == "mysql" {
			if spec.Schema.Mysql == nil {
				return nil
			}

			statement, err := mysql.CreateTableStatement(spec.Name, spec.Schema.Mysql)
			if err != nil {
				return err
			}

			statements = append(statements, statement)
		} else if d.Viper.GetString("driver") == "cockroachdb" {
			if spec.Schema.CockroachDB == nil {
				return nil
			}

			statement, err := postgres.CreateTableStatement(spec.Name, spec.Schema.CockroachDB)
			if err != nil {
				return err
			}

			statements = append(statements, statement)
		}

		return nil
	}

	err := filepath.Walk(d.Viper.GetString("input-dir"), handleFile)
	if err != nil {
		fmt.Printf("%#v\n", err)
		return err
	}

	output := strings.Join(statements, ";\n")
	output = fmt.Sprintf("/* Auto generated file. Do not edit by hand. This file was generated by SchemaHero. */\n\n %s;\n\n", output)

	if _, err := os.Stat(d.Viper.GetString("output-dir")); os.IsNotExist(err) {
		os.MkdirAll(d.Viper.GetString("output-dir"), 0755)
	}

	err = ioutil.WriteFile(filepath.Join(d.Viper.GetString("output-dir"), "fixtures.sql"), []byte(output), 0644)
	if err != nil {
		fmt.Printf("%#v\n", err)
		return err
	}

	return nil
}

func (d *Database) PlanSync(filename string) ([]string, error) {
	specContents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file")
	}

	var spec *schemasv1alpha3.TableSpec
	parsedK8sObject := schemasv1alpha3.Table{}
	if err := yaml.Unmarshal(specContents, &parsedK8sObject); err == nil {
		if parsedK8sObject.Spec.Schema != nil {
			spec = &parsedK8sObject.Spec
		}
	}

	if spec == nil {
		plainSpec := schemasv1alpha3.TableSpec{}
		if err := yaml.Unmarshal(specContents, &plainSpec); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal spec")
		}

		spec = &plainSpec
	}

	if spec.Schema == nil {
		return []string{}, nil
	}

	uri, err := getURI(d)
	if err != nil {
		return nil, err
	}

	if d.Viper.GetString("driver") == "postgres" {
		return postgres.PlanPostgresTable(uri, spec.Name, spec.Schema.Postgres)
	} else if d.Viper.GetString("driver") == "mysql" {
		return mysql.PlanMysqlTable(d.Viper.GetString("uri"), spec.Name, spec.Schema.Mysql)
	} else if d.Viper.GetString("driver") == "cockroachdb" {
		return postgres.PlanPostgresTable(d.Viper.GetString("uri"), spec.Name, spec.Schema.CockroachDB)
	}

	return nil, errors.New("unknown database driver")
}

func getURI(d *Database) (string, error) {
	if uriRef := d.Viper.GetString("vault-uri-ref"); uriRef != "" {
		b, err := ioutil.ReadFile(uriRef)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	return d.Viper.GetString("uri"), nil
}

func (d *Database) ApplySync(statements []string) error {
	uri, err := getURI(d)
	fmt.Printf("URI is: %s\n", uri)
	if err != nil {
		return err
	}

	if d.Viper.GetString("driver") == "postgres" {
		return postgres.DeployPostgresStatements(uri, statements)
	} else if d.Viper.GetString("driver") == "mysql" {
		return mysql.DeployMysqlStatements(uri, statements)
	} else if d.Viper.GetString("driver") == "cockroachdb" {
		return postgres.DeployPostgresStatements(uri, statements)
	}

	return errors.New("unknown database driver")
}
