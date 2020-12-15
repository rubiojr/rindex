package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v2"
)

var appCommands []*cli.Command
var dbPath string

func main() {

	app := &cli.App{
		Name:     "idutil",
		Commands: []*cli.Command{},
		Version:  "v0.0.1",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dbpath",
				Required:    true,
				Destination: &dbPath,
			},
			&cli.BoolFlag{
				Name:     "debug",
				Aliases:  []string{"d"},
				Usage:    "Enable debugging",
				Required: false,
			},
		},
	}

	listcmd := &cli.Command{
		Name:   "list",
		Usage:  "List IDs",
		Action: listIDs,
	}
	appCommands = append(appCommands, listcmd)

	getcmd := &cli.Command{
		Name:   "get",
		Usage:  "Get ID",
		Action: getID,
	}
	appCommands = append(appCommands, getcmd)

	deletecmd := &cli.Command{
		Name:   "delete",
		Usage:  "Delete ID",
		Action: deleteID,
	}
	appCommands = append(appCommands, deletecmd)

	app.Commands = append(app.Commands, appCommands...)
	err := app.Run(os.Args)
	if err != nil {
		println(fmt.Sprintf("\n🛑 %s", err))
	}
}

func getID(cli *cli.Context) error {
	id := cli.Args().Get(0)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("invalid db path %s", dbPath)
	}
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}
	val, err := db.Get([]byte(id), nil)
	if err == nil {
		var altPaths []string
		json.Unmarshal(val, &altPaths)
		for _, p := range altPaths {
			fmt.Println(" ", p)
		}
	}

	return nil
}

func deleteID(cli *cli.Context) error {
	id := cli.Args().Get(0)
	if id == "" {
		return fmt.Errorf("invalid ID")
	}

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		if string(key) == id {
			fmt.Println("Deleting ID ", id)
			err := db.Delete(key, nil)
			if err != nil {
				panic(err)
			}
			break
		}
	}
	iter.Release()
	return iter.Error()
}

func listIDs(cli *cli.Context) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("invalid db path %s", dbPath)
	}
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		var altPaths []string
		key := iter.Key()
		val := iter.Value()
		json.Unmarshal(val, &altPaths)
		fmt.Printf("%s: %s\n", key, val)
	}
	iter.Release()
	return iter.Error()
}
