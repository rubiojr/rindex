package testutil

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

const REPO_PATH = "tmp-repo"
const REPO_PASS = "test"

func IndexPath() string {
	dir, err := ioutil.TempDir("tmp", "index-*")
	if err != nil {
		panic(err)
	}

	return filepath.Join(dir, "test.idx")
}

func SetupEnv() {
	os.Setenv("RESTIC_REPOSITORY", REPO_PATH)
	os.Setenv("RESTIC_PASSWORD", REPO_PASS)
}

func SetupRepo() {
	SetupEnv()

	for _, p := range []string{"blugeindex/tmp", "tmp"} {
		cmd := exec.Command("rm", "-rf", p)
		if err := cmd.Run(); err != nil {
			panic(err)
		}
	}

	err := os.MkdirAll("tmp", 0755)
	if err != nil {
		panic(err)
	}

	if _, err := os.Stat(REPO_PATH); err == nil {
		return
	}

	cmd := exec.Command("restic", "init")
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	cmd = exec.Command("restic", "backup", "testdata")
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}
