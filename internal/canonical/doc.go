// Package canonical contains the product-independent primitives used by the
// Mahoroba canonical ledger.
//
// The package deliberately does not expose database/sql transactions or a
// generic CRUD store. Canonical changes enter through Command and are executed
// by Writer against a narrowly scoped CanonicalUoW supplied by an adapter.
package canonical
