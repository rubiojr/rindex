package testutil

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

const REPO_PATH = "test-repos/tmp-repo"
const REPO_PASS = "test"

func IndexPath() string {
	dir, err := ioutil.TempDir("tmp", "index-*")
	if err != nil {
		panic(err)
	}

	return filepath.Join(dir, "test.idx")
}

func RandomRepoURI() string {
	dir, err := ioutil.TempDir("test-repos", "repo-*")
	if err != nil {
		panic(err)
	}

	return dir
}

func SetupEnv() {
	os.Setenv("RESTIC_REPOSITORY", REPO_PATH)
	os.Setenv("RESTIC_PASSWORD", REPO_PASS)
}

func ResetEnv() {
	SetupEnv()
}

func CreateSnapshot() {
	cmd := exec.Command("restic", "backup", "testdata")
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func RepoURI() string {
	return os.Getenv("RESTIC_REPOSITORY")
}

func SetupNewRepo() {
	if RepoURI() == "" {
		panic("invalid repo")
	}

	os.RemoveAll(RepoURI())

	for _, p := range []string{"blugeindex/tmp", "tmp"} {
		os.RemoveAll(p)
	}

	err := os.MkdirAll("tmp", 0755)
	if err != nil {
		panic(err)
	}

	if _, err := os.Stat(RepoURI()); err == nil {
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

func SetupRepo() {
	SetupEnv()
	SetupNewRepo()
}
