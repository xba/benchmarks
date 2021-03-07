package databases

import (
	"database/sql"
	"fmt"
	"os/exec"
	"testing"

	_ "github.com/lib/pq"
	bunt "github.com/tidwall/buntdb"
)

type Config struct {
	clear bool

	buntPath        string
	postgresConnect string
}

func init() {
	initBunt(&Config{
		clear:    true,
		buntPath: "../data/bunt.db",
	})
	initPostgres(&Config{
		clear: true,
		postgresConnect: `host=localhost port=5432 user=karl password=karl dbname=benchmarks 
			sslmode=disable`,
	})
	fmt.Println()
}

var (
	buntDB *bunt.DB
)

func initBunt(c *Config) {
	var err error
	if c.clear {
		if err = exec.Command("rm", c.buntPath).Run(); err != nil {
			panic(err)
		}
		fmt.Printf("[bunt] cleared %s\n", c.buntPath)
	}
	if buntDB, err = bunt.Open(c.buntPath); err != nil {
		panic(err)
	}
}

func BenchmarkBuntWrite(b *testing.B) {
	b.SetBytes(1)
	for n := 0; n < b.N; n++ {
		err := buntDB.Update(func(tx *bunt.Tx) error {
			_, _, err := tx.Set(fmt.Sprintf("%d", n), ".", nil)
			return err
		})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkBuntWriteParallel(b *testing.B) {
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for n := 0; pb.Next(); n++ {
			err := buntDB.Update(func(tx *bunt.Tx) error {
				_, _, err := tx.Set(fmt.Sprintf("%d", n), ".", nil)
				return err
			})
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkBuntRead(b *testing.B) {
	b.SetBytes(1)
	for n := 0; n < b.N; n++ {
		buntDB.View(func(tx *bunt.Tx) error {
			_, err := tx.Get(fmt.Sprintf("%d", n))
			return err
		})
	}
}

func BenchmarkBuntReadParallel(b *testing.B) {
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for n := 0; pb.Next(); n++ {
			buntDB.View(func(tx *bunt.Tx) error {
				_, err := tx.Get(fmt.Sprintf("%d", n))
				return err
			})
		}
	})
}

var (
	pgDB *sql.DB
)

func initPostgres(c *Config) {
	var err error
	if pgDB, err = sql.Open("postgres", c.postgresConnect); err != nil {
		panic(err)
	}
	pgDB.SetMaxOpenConns(0)
	if err = pgDB.Ping(); err != nil {
		panic(err)
	}
	if c.clear {
		if _, err = pgDB.Exec(`drop schema public cascade`); err != nil {
			panic(err)
		}
		if _, err = pgDB.Exec(`create schema public`); err != nil {
			panic(err)
		}
		fmt.Printf("[postgres] cleared benchmarks database\n")
	}
	if _, err = pgDB.Exec(`create table if not exists data (
		key bigint not null,
		value bytea not null
	)`); err != nil {
		panic(err)
	}
}

func BenchmarkPostgresWrite(b *testing.B) {
	b.SetBytes(1)
	stmt, err := pgDB.Prepare(`insert into data (key, value) values ($1, $2)`)
	if err != nil {
		panic(err)
	}
	for n := 0; n < b.N; n++ {
		if _, err = stmt.Exec(n, []byte(".")); err != nil {
			panic(err)
		}
	}
}

func BenchmarkPostgresWriteParallel(b *testing.B) {
	b.SetBytes(1)
	stmt, err := pgDB.Prepare(`insert into data (key, value) values ($1, $2)`)
	if err != nil {
		panic(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for n := 0; pb.Next(); n++ {
			if _, err = stmt.Exec(n, []byte(".")); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkPostgresRead(b *testing.B) {
	b.SetBytes(1)
	stmt, err := pgDB.Prepare(`select value from data where key = $1`)
	for n := 0; n < b.N; n++ {
		if _, err = stmt.Exec(n); err != nil {
			panic(err)
		}
	}
}

func BenchmarkPostgresReadParallel(b *testing.B) {
	b.SetBytes(1)
	stmt, err := pgDB.Prepare(`select value from data where key = $1`)
	if err != nil {
		panic(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for n := 0; pb.Next(); n++ {
			if _, err = stmt.Exec(n); err != nil {
				panic(err)
			}
		}
	})

}
