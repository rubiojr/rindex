package main

import (
	"fmt"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v2"
)

var appCommands []*cli.Command
var dbPath string

func main() {

	app := &cli.App{
		Name:     "snutil",
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
		Usage:  "List snapshots",
		Action: listSnapshots,
	}
	appCommands = append(appCommands, listcmd)

	deletecmd := &cli.Command{
		Name:   "delete",
		Usage:  "Delete snapshot",
		Action: deleteSnapshot,
	}
	appCommands = append(appCommands, deletecmd)

	app.Commands = append(app.Commands, appCommands...)
	err := app.Run(os.Args)
	if err != nil {
		println(fmt.Sprintf("\n🛑 %s", err))
	}
}

func deleteSnapshot(cli *cli.Context) error {
	snap := cli.Args().Get(0)
	if snap == "" {
		return fmt.Errorf("invalid snapshot")
	}

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		skey := fmt.Sprintf("%x", key)
		if skey == snap {
			fmt.Println("Deleting snapshot ", snap)
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

func listSnapshots(cli *cli.Context) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("invalid db path %s", dbPath)
	}
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return err
	}
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		fmt.Printf("%x\n", key)
	}
	iter.Release()
	return iter.Error()
}
