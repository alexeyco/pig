# pig

[![Build](https://github.com/alexeyco/pig/actions/workflows/test.yml/badge.svg)](https://github.com/alexeyco/pig/actions/workflows/test.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/alexeyco/pig)](https://pkg.go.dev/github.com/alexeyco/pig)
[![Go Report Card](https://goreportcard.com/badge/github.com/alexeyco/pig)](https://goreportcard.com/report/github.com/alexeyco/pig)
[![Coverage Status](https://coveralls.io/repos/github/alexeyco/pig/badge.svg?branch=main)](https://coveralls.io/github/alexeyco/pig?branch=main)

Simple [pgx](https://github.com/jackc/pgx) wrapper to execute and [scan](https://github.com/alexeyco/pig) query
results.

## Features

* All-in-one tool;
* Simple transactions management:
    * You can set `idle_in_transaction_session_timeout` local
      option ([read more](https://www.postgresql.org/docs/9.6/runtime-config-client.html)),
    * You can set `statement_timeout` local
      option ([read more](https://www.postgresql.org/docs/9.6/runtime-config-client.html)).

## Usage

### Execute query

```go
package main

import (
	"context"
	"log"

	"github.com/alexeyco/pig"
	"github.com/jackc/pgx/v4"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	affectedRows, err := p.Query().Exec("DELETE FROM things WHERE id = $1", 123)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("affected", affectedRows, "rows")
}
```

### Get single entity

```go
package main

import (
	"context"
	"log"

	"github.com/alexeyco/pig"
	"github.com/jackc/pgx/v4"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	var cnt int64
	err = p.Query().Get(&cnt, "SELECT count(*) FROM things")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(cnt, "things found")
}
```

### Select multiple entities

```go
package main

import (
	"context"
	"log"

	"github.com/alexeyco/pig"
	"github.com/jackc/pgx/v4"
)

type Thing struct {
	ID       int64  `db:"id"`
	Name     string `db:"name"`
	Quantity int64  `db:"quantity"`
}

func main() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	var things []Thing
	err = p.Query().Select(&things, "SELECT * FROM things")
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(things)
}
```

### Make transactions

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/alexeyco/pig"
	"github.com/jackc/pgx/v4"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "")
	if err != nil {
		log.Fatalln(err)
	}

	p := pig.New(conn)

	var affectedRows int64
	err = p.Tx(pig.TransactionTimeout(time.Second)).
		Exec(func(ex *pig.Ex) error {
			affectedRows, err = p.Query().Exec("DELETE FROM things WHERE id = $1", 123)
			if err != nil {
				return err
			}

			return nil
		})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("affected", affectedRows, "rows")
}
```

## License

```
MIT License

Copyright (c) 2021 Alexey Popov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
