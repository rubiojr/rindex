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

	seedRepo(dir, REPO_PASS)
	return dir
}

func CreateSnapshotForRepo(path, pass string) {
	cmd := exec.Command("restic", "backup", "testdata")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "RESTIC_REPOSITORY="+path)
	cmd.Env = append(cmd.Env, "RESTIC_PASSWORD="+pass)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func CreateSnapshot() {
	CreateSnapshotForRepo(REPO_PATH, REPO_PASS)
}

func RepoWithPathPass(path, pass string) {
	os.RemoveAll(path)

	for _, p := range []string{"blugeindex/tmp", "tmp"} {
		os.RemoveAll(p)
	}

	err := os.MkdirAll("tmp", 0755)
	if err != nil {
		panic(err)
	}

	if _, err := os.Stat(path); err == nil {
		return
	}
	seedRepo(path, pass)
}

func SetupNewRepo() {
	RepoWithPathPass(REPO_PATH, REPO_PASS)
}

func seedRepo(path, pass string) {
	cmd := exec.Command("restic", "init")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "RESTIC_REPOSITORY="+path)
	cmd.Env = append(cmd.Env, "RESTIC_PASSWORD="+pass)
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	cmd = exec.Command("restic", "backup", "testdata")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "RESTIC_REPOSITORY="+path)
	cmd.Env = append(cmd.Env, "RESTIC_PASSWORD="+pass)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func SetupRepo() {
	SetupNewRepo()
}
