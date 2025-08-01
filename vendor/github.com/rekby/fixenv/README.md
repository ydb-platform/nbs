[![Go Reference](https://pkg.go.dev/badge/github.com/rekby/fixenv.svg)](https://pkg.go.dev/github.com/rekby/fixenv)
[![Coverage Status](https://coveralls.io/repos/github/rekby/fixenv/badge.svg?branch=master)](https://coveralls.io/github/rekby/fixenv?branch=master)
[![GoReportCard](https://goreportcard.com/badge/github.com/rekby/fixenv)](https://goreportcard.com/report/github.com/rekby/fixenv)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  

Go Fixtures
===========

Inspired by pytest fixtures.

[Examples](https://github.com/rekby/fixenv/tree/master/examples)

The package provide engine for write and use own fixtures.

Fixture - function-helper for provide some object/service for test. 
Fixture calls with same parameters cached and many time calls of the fixture return same result 
and work did once only.

```golang
package example

// counter fixture - increment globalCounter every non cached call
// and return new globalCounter value
// cache shared one test 
func counter(e fixenv.Env) int {...}

func TestCounter(t *testing.T) {
	e := fixenv.NewEnv(t)

	r1 := counter(e)
	r2 := counter(e)
	if r1 != r2 {
		t.Error()
	}

	t.Run("subtest", func(t *testing.T) {
		e := fixenv.NewEnv(t)
		r3 := counter(e)
		if r3 == r1 {
			t.Error()
		}
	})
}
```


For example with default scope test - it work will done once per test. 
With scope TestAndSubtests - cache shared by test (TestFunction(t *testing.T)) and all of that subtest.
With package scope - result cached for all test in package.
Fixture can have cleanup function, that called while out of scope.

Fixture can call other fixtures, cache shared between them.

For example simple account test:
```golang
package example

// db create database abd db struct, cached per package - call
// once and same db shared with all tests
func db(e Env)*DB{...}

// DbCustomer - create customer with random personal data
// but fixed name. Fixture result shared by test and subtests, 
// then mean many calls Customer with same name will return same
// customer object.
// Call Customer with other name will create new customer
// and resurn other object.
func DbCustomer(e Env, name string) Customer {
	// ... create customer
	db(e).CustomerStore(cust)
	// ...
	return cust
}

// DbAccount create bank account for customer with given name.
func DbAccount(e Env, customerName, accountName string)Account{
	cust := DbCustomer(e, customerName)
	// ... create account
	db(e).AccountStore(acc)
	// ...
	return acc
}

func TestFirstOwnAccounts(t *testing.T){
	e := NewEnv(t)
	// background:
	// create database
	// create customer bob 
	// create account from
	accFrom := DbAccount(e, "bob", "from")
	
	// get existed db, get existed bob, create account to
	accTo := DbAccount(e, "bob", "to")
	
	PutMoney(accFrom, 100)
	SendMoney(accFrom, accTo, 20)
	if accFrom != 80 {
		t.Error()
	}
	if accTo != 20 {
		t.Error()   
	}
	
	// background:
	// delete account to
	// delete account from
	// delete customer bob
}

func TestSecondTransferBetweenCustomers(t *testing.T){
	e := NewEnv(t)
	
	// background:
	// get db, existed from prev test
	// create customer bob
	// create account main for bob
	accFrom := DbAccount(e, "bob", "main")
	
	// background:
	// get existed db
	// create customer alice
	// create account main for alice
	accTo := DbAccount(e, "alice", "main")
	PutMoney(accFrom, 100)
	SendMoney(accFrom, accTo, 20)
	if accFrom != 80 {
		t.Error()
	}
	if accTo != 20 {
		t.Error()
	}
	
	// background:
	// remove account of alice
	// remove customer alice
	// remove account of bob
	// remove customer bob
}

// background:
// after all test finished drop database
```
