```
x390
commit e1a1d55e43693543931bae5df2be7b50d5cb49ee (HEAD -> new-api, origin/new-api)
Author: Sergio Rubio <rubiojr@frameos.org>
Date:   Mon Nov 16 22:07:06 2020 +0100
dataset: https://mono.s3.filebase.com/restic.tgz

Indexed nodes: 91247
Already indexed nodes: 2104
goos: linux
goarch: amd64
pkg: github.com/rubiojr/rindex
BenchmarkIndex-8                     1  303201445396 ns/op  57036488088 B/op  393555842 allocs/op
BenchmarkIndexBatch100-8             1  8231955217 ns/op    2295889216 B/op   36436167 allocs/op
BenchmarkIndexBatch1000-8            1  8374405386 ns/op    2299962280 B/op   36511738 allocs/op
PASS
ok    github.com/rubiojr/rindex 319.832s
```
